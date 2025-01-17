# ETL CLI Documentation

## Overview

A command line tool for managing ETL (Extract, Transform, Load) operations
for financial data from Tiingo's APIs, including end-of-day prices and fundamentals data,
loading the data to DuckDB or MotherDuck tables, as per the config.*.yaml files.

This tool provides commands for:
- Managing end-of-day price data (eod)
- Managing fundamental data (fundamentals)

## End-of-Day (EOD) Commands

### eod daily

```
Usage:
  etl eod daily

```

### eod backfill

```
Usage:
  etl eod backfill [tickers]

```

## Fundamentals Commands

### fundamentals daily

```
Usage:
  etl fundamentals daily [--tickers TICKER1,TICKER2,...] [--skipTickers TICKER1,TICKER2,...] [--halfOnly] [--lookback DAYS] [--batchSize SIZE] [--skipExisting] [flags]

Flags:
      --batchSize int        Process tickers in batches of this size (0 means process all at once)
      --halfOnly             Process only half of the tickers based on current hour (even=first half, odd=second half)
      --lookback int         Number of days to look back for updates (0 means no filter)
      --skipExisting         Skip tickers that already exist in the database
      --skipTickers string   Comma-separated list of tickers to skip (e.g., BAD1,BAD2)
      --tickers string       Comma-separated list of tickers (e.g., AAPL,MSFT,GOOGL)

```

### fundamentals metadata

```
Usage:
  etl fundamentals metadata

```

### fundamentals statements

```
Usage:
  etl fundamentals statements [--tickers TICKER1,TICKER2,...] [--skipTickers TICKER1,TICKER2,...] [--halfOnly] [--lookback DAYS] [--batchSize SIZE] [--skipExisting] [flags]

Flags:
      --batchSize int        Process tickers in batches of this size (0 means process all at once)
      --halfOnly             Process only half of the tickers based on current hour (even=first half, odd=second half)
      --lookback int         Number of days to look back for updates (0 means no filter)
      --skipExisting         Skip tickers that already exist in the database
      --skipTickers string   Comma-separated list of tickers to skip (e.g., BAD1,BAD2)
      --tickers string       Comma-separated list of tickers (e.g., AAPL,MSFT,GOOGL)

```

