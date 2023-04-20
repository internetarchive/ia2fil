#!/usr/bin/env python3

import glob
import os
import random

from collections import defaultdict
from datetime import datetime, date, timedelta, timezone

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
DBQS = {
    "active_or_published_total_size": """
        SELECT PG_SIZE_PRETTY ( SUM ( 1::BIGINT << sq.claimed_log2_size ) ) FROM
        (
            SELECT DISTINCT(piece_id), claimed_log2_size FROM published_deals
	    WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            --AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00')
        ) sq;
    """,
    "active_or_published_label_size": """
        SELECT PG_SIZE_PRETTY ( SUM ( 1::BIGINT << sq.claimed_log2_size ) ) FROM
        (
            SELECT DISTINCT(decoded_label), claimed_log2_size FROM published_deals
	    WHERE provider_id = '02011071'
            AND (status = 'active' OR status = 'published')
            /* AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00') */ /* uncomment this to search before stated date */
        ) sq;
    """,
    "provider_item_counts": """
        SELECT provider_id, count(1) AS cnt FROM naive.published_deals WHERE client_id = '01131298' GROUP BY provider_id ORDER BY cnt DESC;
    """,
    "deal_count_by_status": """
        SELECT status, count(1) FROM published_deals WHERE client_id = '01131298' GROUP BY status;
    """,
    "proven_active_or_published_total_size": """
        SELECT PG_SIZE_PRETTY ( SUM ( 1::BIGINT << proven_log2_size ) ) FROM pieces
        WHERE piece_id IN (
            SELECT (piece_id) FROM published_deals
            WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            --AND entry_created > '2023-03-22 00:00:00.00'
        );
    """,
    "terminated_deal_count_by_reason": """
        SELECT published_deal_meta->>'termination_reason' AS reason, count(1) FROM published_deals WHERE client_id = '01131298' AND status = 'terminated' GROUP BY reason;
    """,
    "index_age": """
        SELECT ts_from_epoch( ( metadata->'market_state'->'epoch' )::INTEGER ) FROM global;
    """
}


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
def load_oracle(dbq):
    with psycopg2.connect(database=os.getenv("DBNAME"), host=os.getenv("DBHOST"), user=os.getenv("DBUSER"), password=os.getenv("DBPASS"), port=os.getenv("DBPORT")) as conn:
        conn.cursor().execute(DBSP)
        return pd.read_sql_query(dbq, conn)


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

pub_size = load_oracle(DBQS["active_or_published_total_size"])
prvn_size = load_oracle(DBQS["proven_active_or_published_total_size"])

cols = st.columns(4)
cols[0].metric("Ready Files", f"{len(upld):,}", f"{len(d)-len(upld):,}", delta_color="inverse")
cols[1].metric("Ready Size", humanize(upld.Size.sum()), humanize(d.Size.sum()-upld.Size.sum()), delta_color="inverse")
cols[2].metric("Recent Activity", dkey.strftime("%b %d"), f"{tdlt} {'days' if tdlt > 1 else 'day'} ago" if tdlt else "today", delta_color="off")
cols[3].metric("Filoracle", pub_size.iloc[0,0], prvn_size.iloc[0,0])

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

tbs = st.tabs(["Accumulated", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly", "Status", "Data"])

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

pro_ct = load_oracle(DBQS["provider_item_counts"]).rename(columns={"provider_id": "Provider", "cnt": "Count"})
dl_st_ct = load_oracle(DBQS["deal_count_by_status"]).rename(columns={"status": "Status", "count": "Count"})
trm_ct = load_oracle(DBQS["terminated_deal_count_by_reason"]).rename(columns={"reason": "Reason", "count": "Count"}).replace("deal no longer part of market-actor state", "expired").replace("entered on-chain final-slashed state", "slashed")
idx_age = load_oracle(DBQS["index_age"])

cols = tbs[6].columns(2)
with cols[0]:
    ch = alt.Chart(dl_st_ct).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Status:N", scale=alt.Scale(domain=["active", "published", "terminated"], range=["teal", "orange", "red"]), legend=alt.Legend(title="Deal Status")),
        tooltip=["Status:N", alt.Tooltip("Count:Q", format=",")]
    )
    st.altair_chart(ch, use_container_width=True)
with cols[1]:
    ch = alt.Chart(trm_ct).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Reason:N", scale=alt.Scale(domain=["expired", "slashed"], range=["orange", "red"]), legend=alt.Legend(title="Termination Reason")),
        tooltip=["Reason:N", alt.Tooltip("Count:Q", format=",")]
    )
    st.altair_chart(ch, use_container_width=True)

cols = tbs[7].columns(3)
with cols[0]:
    st.caption("Daily Ready Sizes")
    st.dataframe(t[["PTime", "Size"]].sort_values(by="PTime", ascending=False).style.format({"PTime":lambda t: t.strftime("%Y-%m-%d"), "Size": "{:,.0f}"}), use_container_width=True)
with cols[1]:
    st.caption("Service Providers")
    st.dataframe(pro_ct.style.format({"Count": "{:,}"}), use_container_width=True)
with cols[2]:
    st.caption("Deal Status")
    st.dataframe(dl_st_ct, use_container_width=True)
    st.caption("Termination Reason")
    st.dataframe(trm_ct, use_container_width=True)
    st.write(f"_Updated: {(datetime.now(timezone.utc) - idx_age.iloc[0,0]).total_seconds():,.0f} seconds ago._")

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
