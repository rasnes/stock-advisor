import datetime

import streamlit as st
import ibis
import polars as pl

from utils import duck

# This must be the first Streamlit command
st.set_page_config(layout="wide")

# Get query parameters
query_params = st.query_params

md_daily_adjusted = duck.ibis_con.table("daily_adjusted")
daily = duck.Daily(md_daily_adjusted)


# Cache all_tickers
@st.cache_data(ttl=3600)
def get_all_tickers():
    return daily.get_all_tickers().execute()


all_tickers = get_all_tickers()

col1, col2 = st.columns(2)

# Get date parameters from URL or use defaults
default_from = datetime.date(2024, 1, 1)
default_to = datetime.datetime.now().date()

# Parse date from query params if available
try:
    if "date_from" in query_params:
        date_from_str = query_params["date_from"]
        default_from = datetime.datetime.strptime(date_from_str, "%Y-%m-%d").date()

    if "date_to" in query_params:
        date_to_str = query_params["date_to"]
        default_to = datetime.datetime.strptime(date_to_str, "%Y-%m-%d").date()
except ValueError:
    # If date parsing fails, use defaults
    pass

with col1:
    date_from = st.date_input(
        label="From",
        value=default_from,
        min_value=datetime.date(1995, 1, 1),
        max_value=datetime.datetime.now()
    )

with col2:
    date_to = st.date_input(
        label="To",
        value=default_to,
        min_value=datetime.date(1995, 1, 1),
        max_value=datetime.datetime.now()
    )

# Update query params with selected dates
query_params["date_from"] = date_from.strftime("%Y-%m-%d")
query_params["date_to"] = date_to.strftime("%Y-%m-%d")

# Get default tickers from query params if available
default_tickers = ["AAPL", "GOOGL"]
if "tickers" in query_params:
    param_tickers = query_params["tickers"].split(",")
    # Verify these tickers exist in our dataset
    default_tickers = [ticker for ticker in param_tickers if ticker in all_tickers.ticker.to_list()]
    if not default_tickers:  # If none of the tickers are valid, use the original defaults
        default_tickers = ["AAPL", "GOOGL"]

# Initialize session state for ticker selection
if "ticker_selection" not in st.session_state:
    st.session_state.ticker_selection = default_tickers

# Define callback function to update query params
def update_ticker_params():
    if st.session_state.ticker_select:
        query_params["tickers"] = ",".join(st.session_state.ticker_select)
    elif "tickers" in query_params:
        del query_params["tickers"]

# Multiselect widget with callback
selected_tickers = st.multiselect(
    label="Select tickers",
    options=all_tickers.ticker,
    default=default_tickers,
    key="ticker_select",
    on_change=update_ticker_params
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
