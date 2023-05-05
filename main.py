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
    "EndOfTerm2012WebCrawls": "End of Term 2012 Web Crawls",
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
        SELECT PG_SIZE_PRETTY (SUM (1::BIGINT << sq.claimed_log2_size))
        FROM (
            SELECT DISTINCT(piece_id), claimed_log2_size
            FROM published_deals
	    WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            AND entry_created BETWEEN '{fday}' AND '{lday}'
            --AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00')
        ) sq;
    """,
    "active_or_published_daily_size": """
        SELECT DATE_TRUNC('day', sq.entry_created) AS dy, SUM((1::BIGINT << sq.claimed_log2_size) / 1024 / 1024 / 1024) AS size, COUNT(sq.claimed_log2_size) AS pieces
        FROM (
            SELECT DISTINCT ON(piece_id) piece_id, entry_created, claimed_log2_size
            FROM published_deals
            WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            AND entry_created BETWEEN '{fday}' AND '{lday}'
            ORDER BY piece_id, entry_created
        ) sq
        GROUP BY DATE_TRUNC('day', sq.entry_created);
    """,
    "active_or_published_label_size": """
        SELECT PG_SIZE_PRETTY (SUM (1::BIGINT << sq.claimed_log2_size))
        FROM (
            SELECT DISTINCT(decoded_label), claimed_log2_size
            FROM published_deals
	    WHERE provider_id = '02011071'
            AND (status = 'active' OR status = 'published')
            AND entry_created BETWEEN '{fday}' AND '{lday}'
            /* AND start_epoch < epoch_from_ts('2022-12-13 20:07:00+00') */ /* uncomment this to search before stated date */
        ) sq;
    """,
    "provider_item_counts": """
        SELECT provider_id, count(1) AS cnt
        FROM published_deals
        WHERE client_id = '01131298'
        AND entry_created BETWEEN '{fday}' AND '{lday}'
        GROUP BY provider_id
        ORDER BY cnt DESC;
    """,
    "deal_count_by_status": """
        SELECT status, count(1)
        FROM published_deals
        WHERE client_id = '01131298'
        AND entry_created BETWEEN '{fday}' AND '{lday}'
        GROUP BY status;
    """,
    "copies_count_size": """
        SELECT sq.copies, COUNT(sq.copies), SUM((1::BIGINT << sq.sz) / 1024 / 1024 / 1024) AS size
        FROM (
            SELECT COUNT(piece_id) AS copies, MAX(claimed_log2_size) AS sz
            FROM published_deals
            WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            AND entry_created BETWEEN '{fday}' AND '{lday}'
            GROUP BY piece_id
        ) sq
        GROUP BY copies;
    """,
    "proven_active_or_published_total_size": """
        SELECT PG_SIZE_PRETTY (SUM (1::BIGINT << proven_log2_size))
        FROM pieces
        WHERE piece_id IN (
            SELECT (piece_id)
            FROM published_deals
            WHERE client_id = '01131298'
            AND (status = 'active' OR status = 'published')
            AND entry_created BETWEEN '{fday}' AND '{lday}'
            --AND entry_created > '2023-03-22 00:00:00.00'
        );
    """,
    "terminated_deal_count_by_reason": """
        SELECT published_deal_meta->>'termination_reason' AS reason, count(1)
        FROM published_deals
        WHERE client_id = '01131298'
        AND status = 'terminated'
        AND entry_created BETWEEN '{fday}' AND '{lday}'
        GROUP BY reason;
    """,
    "index_age": """
        SELECT ts_from_epoch( ( metadata->'market_state'->'epoch' )::INTEGER )
        FROM global;
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


def temporal_bars(data, bin, period, ylim, state):
    ch = alt.Chart(data, height=250)
    ch = ch.mark_bar(color="#ff2b2b") if state == "Onchain" else ch.mark_bar()
    return ch.encode(
        x=alt.X(f"{bin}(Day):T", title=period),
        y=alt.Y(f"sum({state}):Q", axis=alt.Axis(format=",.0f"), title=f"{state} Size", scale=alt.Scale(domain=[0, ylim])),
        tooltip=[alt.Tooltip(f"{bin}(Day):T", title=period), alt.Tooltip("sum(Packed):Q", format=",.0f", title="Packed"), alt.Tooltip("sum(Onchain):Q", format=",.0f", title="Onchain")]
    ).interactive(bind_y=False).configure_axisX(grid=False)


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

fdf = iad.CARTime.min().date()
ldf = datetime.today().date()
fday, lday = st.slider("Date Range", value=(fdf, ldf), min_value=fdf, max_value=ldf)
lday = lday + timedelta(1)

iad = iad[(iad.CARTime>=pd.to_datetime(fday)) & (iad.CARTime<=pd.to_datetime(lday))]

d = pd.merge(iad, ls, left_on="File", right_on="File", how="left")

# Temporary hack to backfill old data
d["PTime"].mask(d.Collection.isin(FINISHED), d.CARTime, inplace=True)

upld = d[~d["PTime"].isnull()]
if not len(upld):
    st.warning(f"No files are packed from collection: `{col}`")
    st.stop()
t = upld.resample("D", on="PTime").sum().reset_index()

rt = upld[["PTime", "Size"]].set_index("PTime").sort_index()
last = rt.last("D")
dkey = last.index[-1].date()

c = d[["Collection", "Size"]].groupby("Collection").sum().reset_index()

tdlt = (date.today() - dkey).days

cp_ct_sz = load_oracle(DBQS["copies_count_size"].format(fday=fday, lday=lday)).rename(columns={"copies": "Copies", "count": "Count", "size": "Size"})
dsz = load_oracle(DBQS["active_or_published_daily_size"].format(fday=fday, lday=lday)).rename(columns={"dy": "PTime", "size": "Onchain", "pieces": "Pieces"})
dsz["PTime"] = pd.to_datetime(dsz.PTime).dt.tz_localize(None)
msz = pd.merge(t[["PTime", "Size"]], dsz, left_on="PTime", right_on="PTime", how="outer").rename(columns={"PTime": "Day", "Size": "Packed"}).sort_values(by="Day", ascending=False).fillna(0)

cols = st.columns(4)
cols[0].metric("Packed", humanize(upld.Size.sum()), f"{len(upld):,} files", help="Total packed CAR files in the Internet Archive")
cols[1].metric("On-chain", humanize(cp_ct_sz.Size.sum()), f"{cp_ct_sz.Count.sum():,.0f} files", help="Total unique active/published pieces in the Filecoin network")
cols[2].metric("4+ Replications", humanize(cp_ct_sz[cp_ct_sz.Copies>=4].Size.sum()), f"{cp_ct_sz[cp_ct_sz.Copies>=4].Count.sum():,.0f} files", help="Unique active/published pieces with at least four replications in the Filecoin network")
cols[3].metric("Recent Activity", dkey.strftime("%b %d"), f"{tdlt} days ago" if tdlt > 1 else "yesterday" if tdlt else "today", delta_color="off", help="Last record day in the Spade CSV files")

cols = st.columns(4)
rt = msz.set_index("Day").sort_index()
last = rt.last("D")
cols[0].metric("Last Day", humanize(last.Packed.sum()), humanize(last.Onchain.sum()), help="Total packed and on-chain sizes of unique files of the last day")
last = rt.last("7D")
cols[1].metric("Last Week", humanize(last.Packed.sum()), humanize(last.Onchain.sum()), help="Total packed and on-chain sizes of unique files of the last week")
last = rt.last("30D")
cols[2].metric("Last Month", humanize(last.Packed.sum()), humanize(last.Onchain.sum()), help="Total packed and on-chain sizes of unique files of the last month")
last = rt.last("365D")
cols[3].metric("Last Year", humanize(last.Packed.sum()), humanize(last.Onchain.sum()), help="Total packed and on-chain sizes of unique files of the last year")

tbs = st.tabs(["Accumulated", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly", "Status", "Data"])

rtv = rt[["Packed", "Onchain"]]
ranges = {
    "Day": rtv.groupby(pd.Grouper(freq="D")).sum().to_numpy().max(),
    "Week": rtv.groupby(pd.Grouper(freq="W")).sum().to_numpy().max(),
    "Month": rtv.groupby(pd.Grouper(freq="M")).sum().to_numpy().max(),
    "Quarter": rtv.groupby(pd.Grouper(freq="Q")).sum().to_numpy().max(),
    "Year": rtv.groupby(pd.Grouper(freq="Y")).sum().to_numpy().max()
}

base = alt.Chart(msz).encode(x="Day:T")
ch = alt.layer(
    base.mark_line(size=4).transform_window(
        sort=[{"field": "Day"}],
        TotalPacked="sum(Packed)"
    ).encode(y="TotalPacked:Q"),
    base.mark_line(size=4, color="#ff2b2b").transform_window(
        sort=[{"field": "Day"}],
        TotalOnchain="sum(Onchain)"
    ).encode(y="TotalOnchain:Q")
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[0].altair_chart(ch, use_container_width=True)

ch = temporal_bars(msz, "utcyearmonthdate", "Day", ranges["Day"], "Packed")
tbs[1].altair_chart(ch, use_container_width=True)
ch = temporal_bars(msz, "utcyearmonthdate", "Day", ranges["Day"], "Onchain")
tbs[1].altair_chart(ch, use_container_width=True)

ch = temporal_bars(msz, "yearweek", "Week", ranges["Week"], "Packed")
tbs[2].altair_chart(ch, use_container_width=True)
ch = temporal_bars(msz, "yearweek", "Week", ranges["Week"], "Onchain")
tbs[2].altair_chart(ch, use_container_width=True)

ch = temporal_bars(msz, "yearmonth", "Month", ranges["Month"], "Packed")
tbs[3].altair_chart(ch, use_container_width=True)
ch = temporal_bars(msz, "yearmonth", "Month", ranges["Month"], "Onchain")
tbs[3].altair_chart(ch, use_container_width=True)

ch = temporal_bars(msz, "yearquarter", "Quarter", ranges["Quarter"], "Packed")
tbs[4].altair_chart(ch, use_container_width=True)
ch = temporal_bars(msz, "yearquarter", "Quarter", ranges["Quarter"], "Onchain")
tbs[4].altair_chart(ch, use_container_width=True)

ch = temporal_bars(msz, "year", "Year", ranges["Year"], "Packed")
tbs[5].altair_chart(ch, use_container_width=True)
ch = temporal_bars(msz, "year", "Year", ranges["Year"], "Onchain")
tbs[5].altair_chart(ch, use_container_width=True)

pro_ct = load_oracle(DBQS["provider_item_counts"].format(fday=fday, lday=lday)).rename(columns={"provider_id": "Provider", "cnt": "Count"})
dl_st_ct = load_oracle(DBQS["deal_count_by_status"].format(fday=fday, lday=lday)).rename(columns={"status": "Status", "count": "Count"})
trm_ct = load_oracle(DBQS["terminated_deal_count_by_reason"].format(fday=fday, lday=lday)).rename(columns={"reason": "Reason", "count": "Count"}).replace("deal no longer part of market-actor state", "expired").replace("entered on-chain final-slashed state", "slashed")
idx_age = load_oracle(DBQS["index_age"])

cols = tbs[6].columns((3, 2, 2))
with cols[0]:
    ch = alt.Chart(cp_ct_sz, title="Active/Published Copies").mark_bar().encode(
        x="Count:Q",
        y=alt.Y("Copies:O", sort="-y"),
        tooltip=["Copies:O", alt.Tooltip("Count:Q", format=",")]
    ).configure_axisX(grid=False)
    st.altair_chart(ch, use_container_width=True)
with cols[1]:
    ch = alt.Chart(dl_st_ct).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Status:N", scale=alt.Scale(domain=["active", "published", "terminated"], range=["teal", "orange", "red"]), legend=alt.Legend(title="Deal Status", orient="top")),
        tooltip=["Status:N", alt.Tooltip("Count:Q", format=",")]
    )
    st.altair_chart(ch, use_container_width=True)
with cols[2]:
    ch = alt.Chart(trm_ct).mark_arc().encode(
        theta="Count:Q",
        color=alt.Color("Reason:N", scale=alt.Scale(domain=["expired", "slashed"], range=["orange", "red"]), legend=alt.Legend(title="Termination Reason", orient="top")),
        tooltip=["Reason:N", alt.Tooltip("Count:Q", format=",")]
    )
    st.altair_chart(ch, use_container_width=True)

cols = tbs[7].columns((6, 4, 4, 3))
with cols[0]:
    st.caption("Daily Activity")
    st.dataframe(msz.style.format({"Day":lambda t: t.strftime("%Y-%m-%d"), "Packed": "{:,.0f}", "Onchain": "{:,.0f}", "Pieces": "{:,.0f}"}), use_container_width=True)
with cols[1]:
    st.caption("Service Providers")
    st.dataframe(pro_ct.style.format({"Provider": "f0{}", "Count": "{:,}"}), use_container_width=True)
with cols[2]:
    st.caption("Active/Published Copies")
    st.dataframe(cp_ct_sz.set_index(cp_ct_sz.columns[0]).style.format({"Count": "{:,}", "Size": "{:,.0f}"}), use_container_width=True)
with cols[3]:
    st.caption("Deal Status")
    st.dataframe(dl_st_ct.set_index(dl_st_ct.columns[0]), use_container_width=True)
    st.caption("Termination Reason")
    st.dataframe(trm_ct.set_index(trm_ct.columns[0]), use_container_width=True)
    st.write(f"_Updated: {(datetime.now(timezone.utc) - idx_age.iloc[0,0]).total_seconds()/60:,.0f} minutes ago._")

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
    st.dataframe(d, use_container_width=True)
