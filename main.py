#!/usr/bin/env python3

import glob
import os
import random

from collections import defaultdict
from datetime import datetime, date, timedelta

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


@st.cache_data(ttl=3600, show_spinner="Loading metadata...")
def load_data(col):
#    sr = search_items(f"collection:{col} format:(Content Addressable aRchive) -format:Trigger", params={"service": "files"}, fields=["identifier,format,mtime,size,root_cid"])
    sr = search_items(f"collection:{col} format:(Content Addressable aRchive) -format:Log -format:Trigger", params={"service": "files"}, fields=["identifier,name,mtime,size"])
#    fl = defaultdict(dict)
    fl = defaultdict(dict)
    for r in sr:
#        id = f"{r['identifier']}/{r['name']}"
        id = r["name"]
        fl[id]["Item"] = r["identifier"]
        fl[id]["Collection"] = col
        fl[id]["CARTime"] = datetime.fromtimestamp(r.get("mtime"))
        fl[id]["Size"] = r.get("size") / 1024 / 1024 / 1024
#        f = r["format"]
#        if f == "Content Addressable aRchive":
#            fl[id]["CARTime"] = datetime.fromtimestamp(r.get("mtime"))
#            fl[id]["Size"] = r.get("size") / 1024 / 1024 / 1024
#        if f == "Content Addressable aRchive Log":
#            fl[id]["CID"] = r.get("root_cid")
#            fl[id]["LogTime"] = datetime.fromtimestamp(r.get("mtime"))
#    return pd.DataFrame.from_dict(fl, orient="index").reset_index().rename(columns={"index": "Item"})[["Collection", "Item", "Size", "LogTime", "CARTime", "CID"]]
    return pd.DataFrame.from_dict(fl, orient="index").reset_index().rename(columns={"index": "File"})[["Collection", "Item", "File", "Size", "CARTime"]]


def load_spade(id):
    download(identifier=id, destdir=SPADECACHE, no_directory=True, checksum=True)
    csvf = glob.glob(os.path.join(SPADECACHE, "*.csv"))
    sp = pd.concat((pd.read_csv(f) for f in csvf), ignore_index=True)
    sp["PTime"] = pd.to_datetime(sp["timestamp"].str[:-2])
    sp["PSize"] = sp["padded piece size"] / 1024 / 1024 / 1024
    sp["File"] = sp.url.str.rsplit("/", n=1, expand=True)[[1]]
#    sp["File"] = sp.url.str.replace("https://archive.org/download/", "")
    sp["CID"] = sp["root_cid"]
    return sp[["File", "PSize", "PTime", "CID"]]
#    sr = search_items(f"identifier:{id} format:(Comma-Separated Values)", params={"service": "files"}, fields=["identifier,name"])
#    return [f"https://archive.org/download/{r['identifier']}/{r['name']}" for r in sr]


col = st.selectbox("Collection", options=COLS, format_func=lambda c: COLS[c], key="col")

ls = load_spade("ia-fil-spade-api")

if not col:
    st.stop()

if col == "ALL":
    iad = pd.concat([load_data(c) for c in list(COLS)[1:]], ignore_index=True)
#    iad.set_index("File")
    iad = iad[~iad.File.duplicated(keep="first")]
#    iad.reset_index(inplace=True, drop=True) #.drop_index()
else:
    iad = load_data(col)

if not len(iad):
    st.warning("No files found!")
    st.stop()

#iad.set_index("File")
#ls.set_index("File")

#iad.reset_index(inplace=True, drop=True)
#ls.reset_index(inplace=True, drop=True)

#iad
#d = pd.concat([iad.set_index("File"), ls.set_index("File")], axis=1, join="inner").reset_index()
#d = pd.concat([iad.set_index("File"), ls.set_index("File")], axis=1, join="inner").reset_index()
d = pd.merge(iad, ls, left_on="File", right_on="File", how="left") #.reset_index()

d["PTime"].mask(d.Collection.isin(FINISHED), d.CARTime, inplace=True)

upld = d[~d["PTime"].isnull()]
if not len(upld):
    st.warning(f"No files are ready from collection: `{col}`")
    st.stop()
#m = d[d["File"].isnull()]
t = upld.resample("D", on="PTime").sum().reset_index()
last = t.iloc[-1]
c = d[["Collection", "Size"]].groupby("Collection").sum().reset_index()

#deltai = f"-{len(upld)}" if len(m) else "100%"
#deltas = f"-{m.Size.sum():,.2f} GB" if len(m) else "100%"
tdlt = (date.today() - last.PTime.date()).days

cols = st.columns(3)
cols[0].metric("Ready Files", f"{len(upld):,}", f"{len(d)-len(upld):,}", delta_color="inverse")
cols[1].metric("Ready Size", f"{upld.Size.sum():,.0f} GB", f"{d.Size.sum()-upld.Size.sum():,.0f} GB", delta_color="inverse")
cols[2].metric("Recent Activity", f"{last.PTime.date()}", f"{tdlt} days ago" if tdlt else "today", delta_color="off")
#cols[2].metric("Last Day", f"{last.Size:,.2f} GB", f"{last.PTime.date()}", delta_color="off")
#cols[3].metric("Spade", f"{ls['PSize'].sum():,.0f} GB", f"{len(ls):,} (files)")

cols = st.columns(3)
rt = upld.set_index("PTime").sort_index()
last = rt.last("D")
cols[0].metric("Last Day", f"{last.Size.sum():,.0f} GB", f"{len(last):,} files")
#last = rt.last("W")
last = rt.last("7D")
cols[1].metric("Last Week", f"{last.Size.sum():,.0f} GB", f"{len(last):,} files")
#last = rt.last("M")
last = rt.last("30D")
cols[2].metric("Last Month", f"{last.Size.sum():,.0f} GB", f"{len(last):,} files")

tbs = st.tabs(["Accumulated", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly", "Data"])

brush = alt.selection(type="interval", encodings=["x"], name="sel")

ch = alt.Chart(t).mark_line(
    size=4,
#    point=alt.OverlayMarkDef(color="#e74c3c")
).transform_window(
    Total="sum(Size)"
).encode(
    x="PTime:T",
    y=alt.Y("Total:Q", axis=alt.Axis(format=",.0f")),
    tooltip=["PTime:T", alt.Tooltip("Size:Q", format=",.2f"), alt.Tooltip("Total:Q", format=",.2f")]
).add_selection(
    brush
) #.interactive(bind_y=False).configure_axisX(grid=False)

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

tbs[6].dataframe(t[["PTime", "Size"]].sort_values(by="PTime", ascending=False).style.format({"PTime":lambda t: t.strftime("%Y-%m-%d"), "Size": "{:,.0f}"}), use_container_width=True)

"### Collection Size"
#st.dataframe(c.style.format({"Size": "{:,.0f}"}), use_container_width=True)
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

#if len(m):
#    "### Files Without CIDs"
#    st.dataframe(m.style.format({"Size": "{:,.2f}"}), use_container_width=True)

"### All Files"
st.dataframe(d.style.format({"Size": "{:,.2f}"}), use_container_width=True)