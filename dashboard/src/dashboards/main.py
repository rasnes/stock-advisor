import datetime

import streamlit as st
import ibis
import polars as pl

from utils import duck

st.set_page_config(layout="wide")

md_daily_adjusted = duck.ibis_con.table("daily_adjusted")
daily = duck.Daily(md_daily_adjusted)


# Cache all_tickers
@st.cache_data(ttl=3600)
def get_all_tickers():
    return daily.get_all_tickers().execute()


all_tickers = get_all_tickers()

col1, col2 = st.columns(2)

with col1:
    date_from = st.date_input(
        label="From",
        value=datetime.date(2024, 1, 1),
        min_value=datetime.date(1995, 1, 1),
        max_value=datetime.datetime.now()
    )

with col2:
    date_to = st.date_input(
        label="To",
        value=datetime.datetime.now(),
        min_value=datetime.date(1995, 1, 1),
        max_value=datetime.datetime.now()
    )

# Multiselect widget
selected_tickers = st.multiselect(
    label="Select tickers",
    options=all_tickers.ticker,
    default=["AAPL", "GOOGL"],
    key="ticker_select",
)

# Display the chart using the selected tickers
duck.relative_chart(daily, selected_tickers, date_from, date_to)


# Display predictions and uncertainty
preds = duck.Preds(duck.md_con, duck.md_con.sql(duck.relations["preds_rel"]))
preds_tickers = list(set(selected_tickers).intersection(set(preds.get_all_tickers())))
# Get data for selected tickers
preds.get_df(preds_tickers)
preds.get_forecasts()

preds.plot_preds()


# Create summary table
t: ibis.Table = daily.date_selection(selected_tickers, date_from, date_to)

df_summary = (
    t.to_polars()
    .lazy()
    .sort("date", descending=False)
    .group_by("ticker")
    .agg(
        [
            pl.col("date").last().alias("date"),
            pl.col("adjClose").last().alias("adjClose"),
            pl.col("adjClose").cast(pl.Int64).explode().alias("history"),
        ]
    )
    .with_columns(
        url="https://finance.yahoo.com/quote/" + pl.col("ticker").cast(pl.Utf8)
    )
)

st.dataframe(
    df_summary.collect(),
    column_config={
        "ticker": "Ticker",
        "date": st.column_config.DateColumn("Date"),
        "adjClose": "Adjusted Close",
        "history": st.column_config.LineChartColumn("History", width="small"),
        "url": st.column_config.LinkColumn("Yahoo Finance", display_text="Link"),
    },
)

# Show the raw data below
st.dataframe(preds.forecasts.sort(["pred_col", "ticker"]))
