#!/usr/bin/env python3

import glob
import os
import random

from collections import defaultdict
from datetime import datetime, date, timedelta

import psycopg2

import altair as alt
import streamlit as st
import pandas as pd

from internetarchive import download, search_items


TITLE = "Internet Archive Data to Filecoin"
ICON = "https://archive.org/favicon.ico"
SPADECACHE = "/tmp/spadecsvcache"

os.makedirs(SPADECACHE, exist_ok=True)

st.set_page_config(page_title=TITLE, page_icon=ICON, layout="wide")
st.title(TITLE)

COLS = {
    "ALL": "ALL",
    "EndOfTerm2016PreinaugurationCrawls": "End Of Term 2016 Pre-Inauguration Crawls",
    "EndOfTerm2016PostinaugurationCrawls": "End of Term 2016 Post-Inauguration Crawls",
    "EndOfTerm2016UNTCrawls": "End Of Term 2016 UNT Crawls",
    "EndOfTerm2016LibraryofCongressCrawls": "End Of Term 2016 Library of Congress Crawls",
    "EndOfTerm2020PreElectionCrawls": "End Of Term 2020 Pre Election to Inauguration Crawls",
    "EndOfTerm2020PostInaugurationCrawls": "End Of Term 2020 Post Inauguration Crawls",
    "EndOfTerm2020UNTCrawls": "End of Term 2020 UNT Crawls",
    "archiveteam_ftpgov": "Archive Team Contributed GOV FTP Grabs",
    "prelinger": "Prelinger Archives",
    "prelingerhomemovies": "Prelinger Archives Home Movies",
}

FINISHED = {
    "EndOfTerm2016PreinaugurationCrawls",
    "EndOfTerm2016PostinaugurationCrawls",
    "EndOfTerm2016UNTCrawls",
    "EndOfTerm2016LibraryofCongressCrawls",
    "archiveteam_ftpgov",
    "prelinger",
    "prelingerhomemovies"
}

DBSP = "SET SEARCH_PATH = naive;"
DBQ1 = """
SELECT PG_SIZE_PRETTY ( SUM ( 1::BIGINT << sq.claimed_log2_size ) ) FROM
(
 SELECT DISTINCT(piece_id), claimed_log2_size FROM published_deals
	WHERE client_id = '01131298'
  AND (status = 'active' or status = 'published')
  --AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00')
) sq
;
"""
DBQ2 = """
SELECT PG_SIZE_PRETTY ( SUM ( 1::BIGINT << sq.claimed_log2_size ) ) FROM
(
 SELECT DISTINCT(decoded_label), claimed_log2_size FROM published_deals
	WHERE provider_id = '02011071'
  AND (status = 'active' or status = 'published')
  /* AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00') */ /* uncomment this to search before stated date */
) sq
;
"""
DBQ3 = """
select provider_id, count(1) as cnt from naive.published_deals where client_id = '01131298' group by provider_id order by cnt desc;
"""


@st.cache_data(ttl=3600, show_spinner="Loading File Metadata...")
def load_data(col):
    sr = search_items(f"collection:{col} format:(Content Addressable aRchive) -format:Log -format:Trigger", params={"service": "files"}, fields=["identifier,name,mtime,size"])
    fl = defaultdict(dict)
    for r in sr:
        id = r["name"]
        fl[id]["Item"] = r["identifier"]
        fl[id]["Collection"] = col
        fl[id]["CARTime"] = datetime.fromtimestamp(r.get("mtime"))
        fl[id]["Size"] = r.get("size") / 1024 / 1024 / 1024
    return pd.DataFrame.from_dict(fl, orient="index").reset_index().rename(columns={"index": "File"})[["Collection", "Item", "File", "Size", "CARTime"]]


@st.cache_data(ttl=300, show_spinner="Loading Sapde CSV...")
def load_spade(id):
    download(identifier=id, destdir=SPADECACHE, no_directory=True, checksum=True)
    csvf = glob.glob(os.path.join(SPADECACHE, "*.csv"))
    sp = pd.concat((pd.read_csv(f) for f in csvf), ignore_index=True)
    sp["PTime"] = pd.to_datetime(sp["timestamp"].str[:-2])
    sp["PSize"] = sp["padded piece size"] / 1024 / 1024 / 1024
    sp["File"] = sp.url.str.rsplit("/", n=1, expand=True)[[1]]
    sp["CID"] = sp["root_cid"]
    return sp[["File", "PSize", "PTime", "CID"]]


@st.cache_data(ttl=3600, show_spinner="Loading Oracle Results...")
def load_oracle():
    with psycopg2.connect(database=os.getenv("DBNAME"), host=os.getenv("DBHOST"), user=os.getenv("DBUSER"), password=os.getenv("DBPASS"), port=os.getenv("DBPORT")) as conn:
        conn.cursor().execute(DBSP)
        r1 = pd.read_sql_query(DBQ1, conn)
        r2 = pd.read_sql_query(DBQ2, conn)
        r3 = pd.read_sql_query(DBQ3, conn)
        return (r1, r2, r3)


def humanize(s):
    if s >= 1024:
        return f"{s/1024:,.1f} TB"
    return f"{s:,.1f} GB"


col = st.selectbox("Collection", options=COLS, format_func=lambda c: COLS[c], key="col")

ls = load_spade("ia-fil-spade-api")

if not col:
    st.stop()

if col == "ALL":
    iad = pd.concat([load_data(c) for c in list(COLS)[1:]], ignore_index=True)
    iad = iad[~iad.File.duplicated(keep="first")]
else:
    iad = load_data(col)

if not len(iad):
    st.warning("No files found!")
    st.stop()

fdf, ldf = (datetime.utcfromtimestamp(k.astype(datetime)/1_000_000_000).date() for k in iad.CARTime.sort_values().iloc[[0, -1]].values[:])
fday, lday = st.slider("Date Range", value=(fdf, ldf), min_value=fdf, max_value=ldf)

iad = iad[(iad.CARTime>=pd.to_datetime(fday)) & (iad.CARTime<=pd.to_datetime(lday))]

d = pd.merge(iad, ls, left_on="File", right_on="File", how="left")

d["PTime"].mask(d.Collection.isin(FINISHED), d.CARTime, inplace=True)

upld = d[~d["PTime"].isnull()]
if not len(upld):
    st.warning(f"No files are ready from collection: `{col}`")
    st.stop()
t = upld.resample("D", on="PTime").sum().reset_index()

rt = upld[["PTime", "Size"]].set_index("PTime").sort_index()
last = rt.last("D")
dkey = last.index[-1].date()

c = d[["Collection", "Size"]].groupby("Collection").sum().reset_index()

tdlt = (date.today() - dkey).days

r1, r2, r3 = load_oracle()

cols = st.columns(4)
cols[0].metric("Ready Files", f"{len(upld):,}", f"{len(d)-len(upld):,}", delta_color="inverse")
cols[1].metric("Ready Size", humanize(upld.Size.sum()), humanize(d.Size.sum()-upld.Size.sum()), delta_color="inverse")
cols[2].metric("Recent Activity", f"{dkey}", f"{tdlt} {'days' if tdlt > 1 else 'day'} ago" if tdlt else "today", delta_color="off")
cols[3].metric("Filoracle", r1.iloc[0,0], r2.iloc[0,0])

cols = st.columns(4)
rt = upld[["PTime", "Size"]].set_index("PTime").sort_index()
last = rt.last("D")
cols[0].metric("Last Day", humanize(last.Size.sum()), f"{len(last):,} files")
last = rt.last("7D")
cols[1].metric("Last Week", humanize(last.Size.sum()), f"{len(last):,} files")
last = rt.last("30D")
cols[2].metric("Last Month", humanize(last.Size.sum()), f"{len(last):,} files")
last = rt.last("365D")
cols[3].metric("Last Year", humanize(last.Size.sum()), f"{len(last):,} files")

tbs = st.tabs(["Accumulated", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly", "Data"])

brush = alt.selection(type="interval", encodings=["x"], name="sel")

ch = alt.Chart(t).mark_line(
    size=4,
).transform_window(
    Total="sum(Size)"
).encode(
    x="PTime:T",
    y=alt.Y("Total:Q", axis=alt.Axis(format=",.0f")),
    tooltip=["PTime:T", alt.Tooltip("Size:Q", format=",.2f"), alt.Tooltip("Total:Q", format=",.2f")]
).add_selection(
    brush
)

txt = alt.Chart(t).transform_filter(
    brush
).transform_aggregate(
    total="sum(Size)"
).transform_calculate(
    date_range="sel.PTime ? utcFormat(sel.PTime[0], '%Y-%m-%d') + ' to ' + utcFormat(sel.PTime[1], '%Y-%m-%d') : 'Total'",
    text="datum.date_range + ': ' + format(datum.total, ',.0f') + ' GB'"
).mark_text(
    align="left",
    baseline="top",
    color="#ff4b4b",
    size=18
).encode(
    x=alt.value(20),
    y=alt.value(20),
    text=alt.Text("text:N"),
)

tbs[0].altair_chart(ch + txt, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="utcyearmonthdate(PTime):T",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("utcyearmonthdate(PTime):T", title="Day"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[1].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearweek(PTime):T",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearweek(PTime):T", title="Week"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[2].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearmonth(PTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearmonth(PTime):O", title="Month"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[3].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearquarter(PTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearquarter(PTime):O", title="Quarter"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[4].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="year(PTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("year(PTime):O", title="Year"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[5].altair_chart(ch, use_container_width=True)

cols = tbs[6].columns(2, gap="large")
with cols[0]:
    st.caption("Daily Ready Sizes")
    st.dataframe(t[["PTime", "Size"]].sort_values(by="PTime", ascending=False).style.format({"PTime":lambda t: t.strftime("%Y-%m-%d"), "Size": "{:,.0f}"}), use_container_width=True)
with cols[1]:
    st.caption("Oracle Providers")
    st.dataframe(r3.rename(columns={"provider_id": "Provider", "cnt": "Count"}).style.format({"Count": "{:,}"}), use_container_width=True)


"### Collection Size"
ch = alt.Chart(c).mark_bar().encode(
    x="Size:Q",
    y="Collection:N",
    tooltip=["Collection:N", alt.Tooltip("Size:Q", format=",.0f")]
)
lbl = ch.mark_text(
    align="left",
    baseline="middle",
    color="orange",
    size=16,
    dx=3
).encode(
    text=alt.Text("Size:Q", format=",.0f")
)
st.altair_chart((ch + lbl).configure_axisX(grid=False), use_container_width=True)

if st.button("Show All Files", type="primary"):
    st.dataframe(d.style.format({"Size": "{:,.2f}"}), use_container_width=True)
