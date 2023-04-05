#!/usr/bin/env python3

import os
import random

from collections import defaultdict
from datetime import datetime

import altair as alt
import streamlit as st
import pandas as pd

from internetarchive import search_items


TITLE = "Internet Archive Data to Filecoin"
ICON = "https://archive.org/favicon.ico"


st.set_page_config(page_title=TITLE, page_icon=ICON, layout="wide")
st.title(TITLE)

COLS = {
    "ALL": "ALL",
    "EndOfTerm2016PreinaugurationCrawls": "End Of Term 2016 Pre-Inauguration Crawls",
    "EndOfTerm2016PostinaugurationCrawls": "End of Term 2016 Post-Inauguration Crawls",
    "EndOfTerm2016UNTCrawls": "End Of Term 2016 UNT Crawls",
    "EndOfTerm2016LibraryofCongressCrawls": "End Of Term 2016 Library of Congress Crawls",
    "archiveteam_ftpgov": "Archive Team Contributed GOV FTP Grabs",
    "prelinger": "Prelinger Archives",
    "prelingerhomemovies": "Prelinger Archives Home Movies",
}


@st.cache_data(ttl=3600, show_spinner="Loading metadata...")
def load_data(col):
    sr = search_items(f"collection:{col} format:(Content Addressable aRchive) -format:Trigger", params={"service": "files"}, fields=["identifier,format,mtime,size,root_cid"])
    fl = defaultdict(dict)
    for r in sr:
        id = r["identifier"]
        f = r["format"]
        fl[id]["Collection"] = col
        if f == "Content Addressable aRchive":
            fl[id]["CARTime"] = datetime.fromtimestamp(r.get("mtime"))
            fl[id]["Size"] = r.get("size") / 1024 / 1024 / 1024
        if f == "Content Addressable aRchive Log":
            fl[id]["CID"] = r.get("root_cid")
            fl[id]["LogTime"] = datetime.fromtimestamp(r.get("mtime"))
    return pd.DataFrame.from_dict(fl, orient="index").reset_index().rename(columns={"index": "Item"})[["Collection", "Item", "Size", "LogTime", "CARTime", "CID"]]


col = st.selectbox("Collection", options=COLS, format_func=lambda c: COLS[c], key="col")

if not col:
    st.stop()

if col == "ALL":
    d = pd.concat([load_data(c) for c in list(COLS)[1:]], ignore_index=True)
else:
    d = load_data(col)

if not len(d):
    st.warning("No files found!")
    st.stop()

m = d[d["CID"].isnull()]
t = d.resample("D", on="LogTime").sum().reset_index()
last = t.iloc[-1]
c = d[["Collection", "Size"]].groupby("Collection").sum().reset_index()

deltai = f"-{len(m)}" if len(m) else "100%"
deltas = f"-{m.Size.sum():,.2f} GB" if len(m) else "100%"

cols = st.columns(3)
cols[0].metric("Items", f"{len(d):,}", deltai)
cols[1].metric("Size", f"{d.Size.sum():,.0f} GB", deltas)
cols[2].metric("Last", f"{last.LogTime.date()}", f"{last.Size:,.2f} GB")

tbs = st.tabs(["Accumulated", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly"])

brush = alt.selection(type="interval", encodings=["x"], name="sel")

ch = alt.Chart(t).mark_line(
    size=4,
#    point=alt.OverlayMarkDef(color="#e74c3c")
).transform_window(
    Total="sum(Size)"
).encode(
    x="LogTime:T",
    y=alt.Y("Total:Q", axis=alt.Axis(format=",.0f")),
    tooltip=["LogTime:T", alt.Tooltip("Size:Q", format=",.2f"), alt.Tooltip("Total:Q", format=",.2f")]
).add_selection(
    brush
) #.interactive(bind_y=False).configure_axisX(grid=False)

txt = alt.Chart(t).transform_filter(
    brush
).transform_aggregate(
    total="sum(Size)"
).transform_calculate(
    date_range="sel.LogTime ? utcFormat(sel.LogTime[0], '%Y-%m-%d') + ' to ' + utcFormat(sel.LogTime[1], '%Y-%m-%d') : 'Total'",
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
    x="yearmonthdate(LogTime):T",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearmonthdate(LogTime):T", title="Day"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[1].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearweek(LogTime):T",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearweek(LogTime):T", title="Week"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[2].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearmonth(LogTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearmonth(LogTime):O", title="Month"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[3].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="yearquarter(LogTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("yearquarter(LogTime):O", title="Quarter"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[4].altair_chart(ch, use_container_width=True)

ch = alt.Chart(t).mark_bar().encode(
    x="year(LogTime):O",
    y=alt.Y("sum(Size):Q", axis=alt.Axis(format=",.0f")),
    tooltip=[alt.Tooltip("year(LogTime):O", title="Year"), alt.Tooltip("sum(Size):Q", format=",.0f", title="Size")]
).interactive(bind_y=False).configure_axisX(grid=False)
tbs[5].altair_chart(ch, use_container_width=True)


"### Collection Size"
#st.dataframe(c.style.format({"Size": "{:,.0f}"}), use_container_width=True)
ch = alt.Chart(c).mark_bar().encode(
    x="Size:Q",
    y="Collection:N",
    tooltip=["Collection:N", alt.Tooltip("Size:Q", format=",.0f")]
)
lbl = ch.mark_text(
    align="right",
    baseline="middle",
    color="orange",
    size=16,
    dx=-3
).encode(
    text=alt.Text("Size:Q", format=",.0f")
)
st.altair_chart((ch + lbl).configure_axisX(grid=False), use_container_width=True)

if len(m):
    "### Files Without CIDs"
    st.dataframe(m.style.format({"Size": "{:,.2f}"}), use_container_width=True)

"### All Files"
st.dataframe(d.style.format({"Size": "{:,.2f}"}), use_container_width=True)
