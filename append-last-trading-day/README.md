# append-last-trading-day

Daily batch job running Tuesday-Saturday to get last trading day's end-of-day prices
and appends to the Motherduck table(s).

## TODO


## Extract

Will perform two `GET` requests for data:

1. https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip to get an updated 
overview of tickers available from Tiingo.
2. https://api.tiingo.com/tiingo/daily/prices to get end-of-day prices for all tickers

## Transform

1. Transforms `supported_tickers.csv` to the selected list of tickers of interest. Via the
`view__selected_us_tickers.sql` transform.
2. Semi join results form API request to https://api.tiingo.com/tiingo/daily/prices with the
`selected_us_tickers` VIEW. This filtering makes sure we're not ingesting unneeded data to 
the Motherduck table.

TODO: should the `failed_tickers.csv` be used any way? Currently, I think not, as it will complicate
things and adding

## Load

Ingest the results from Transform.2 above into the Motherduck table.

Use the DuckDB Appender API and log `warning` if primary key already exist (which means
that the ticker-date combination already exists in the database). This strategy assumes that
all data in the Motherduck table is _correct_, which I think is fair; end-of-day prices for yesterday
should never need to be corrected (on Tiingo's end).

Edge case: if `splitFactor` != 1 or `divCash` > 0 for a selected ticker, a reingest of the entire history
for that ticker should be performed (instead of appending it). This is to ensure we get the latest adjusted
prices. For this operation the `INSERT OR REPLACE INTO tbl` strategy will be used for that ticker (which
enables overwriting rows even if there is a violation of a primary key constraint).
