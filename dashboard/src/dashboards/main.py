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
def update_ticker_params() -> None:
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


# Cache trained dates
@st.cache_data(ttl=3600)
def get_all_trained_dates():
    return duck.md_con.sql(duck.relations["trained_dates"]).pl()["trained_date"].sort(descending=True)

all_trained_dates = get_all_trained_dates()

# Get default trained date from query params if available
default_trained_date_index = 0
if "trained_date" in query_params:
    try:
        trained_date_str = query_params["trained_date"]
        # Find the index of this date in our options
        for i, date in enumerate(all_trained_dates):
            if date.strftime("%Y-%m-%d") == trained_date_str:
                default_trained_date_index = i
                break
    except (ValueError, IndexError):
        # If date parsing fails or index is invalid, use default
        pass

# Display the trained date selector
trained_date = st.selectbox(
    label="Trained Date",
    options=all_trained_dates,
    index=default_trained_date_index,
    key="trained_date_select",
    on_change=lambda: query_params.update({"trained_date": st.session_state.trained_date_select.strftime("%Y-%m-%d")})
)

# Update query params with selected trained date
query_params["trained_date"] = trained_date.strftime("%Y-%m-%d")

# Cache the Preds object creation using cache_resource since it contains a database connection
@st.cache_resource(ttl=3600)
def get_preds_object(trained_date_str, is_latest=False):
    """Get cached Preds object for a specific trained date"""
    if is_latest:
        return duck.Preds(duck.md_con, duck.md_con.sql(duck.relations["preds_rel"]))
    else:
        return duck.Preds(duck.md_con, duck.md_con.sql(
            duck.relations["preds_rel_for_date"],
            params={"trained_date": trained_date_str}
        ))

# Cache the actual data retrieval using cache_data
@st.cache_data(ttl=3600)
def get_filtered_data(tickers, trained_date_str, is_latest=False):
    """Cache the filtered data which is the most expensive operation"""
    # Get the Preds object (this call will be cached by cache_resource)
    preds_obj = get_preds_object(trained_date_str, is_latest)

    # Get the filtered data
    preds_obj.get_df(tickers)
    preds_obj.get_forecasts()

    # Return a copy of the forecasts dataframe to ensure it's serializable
    return preds_obj.forecasts.clone()

# Check if we're using the latest trained date
is_latest_date = trained_date.strftime("%Y-%m-%d") == max(all_trained_dates).strftime("%Y-%m-%d")

# Get data for selected tickers
preds = get_preds_object(trained_date, is_latest_date)
preds_tickers = list(set(selected_tickers).intersection(set(preds.get_all_tickers())))

if preds_tickers:
    # Get the filtered data using the cached function
    forecasts_df = get_filtered_data(preds_tickers, trained_date, is_latest_date)

    # Set the forecasts dataframe on the preds object
    preds.forecasts = forecasts_df

    # Plot the predictions
    preds.plot_preds()
else:
    st.warning("No predictions available for the selected tickers.")


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
