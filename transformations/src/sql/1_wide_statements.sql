create or replace table fundamentals.wide_statements as (
  with daily_adjusted_sma as (
    from main.daily_adjusted
    select
      *,
      -- sma(adjClose, ticker, date, 125) as SMA_6m,
      sma(adjClose, ticker, date, 250) as SMA_12m,
      -- sma(adjClose, ticker, date, 500) as SMA_24m,
      sma(adjClose, ticker, date, 750) as SMA_36m,
    -- Use parameter for training data cutoff
    where date <= $cutoff_date
  ), indices as (
    from daily_adjusted_sma
    select
      ticker,
      date,
      adjClose,
      -- SMA_6m,
      SMA_12m,
      -- SMA_24m,
      SMA_36m,
    where ticker = 'SPY'
  ), pivoted_price as (
    pivot indices on ticker
    using first(adjClose) as adjClose,
          -- first(SMA_6m) as SMA_6m,
          first(SMA_12m) as SMA_12m,
          -- first(SMA_24m) as SMA_24m,
          first(SMA_36m) as SMA_36m
  ), daily_subset as (
    select
      da.*,
      sut.exchange,
    from daily_adjusted_sma as da
    join main.selected_us_tickers as sut
      using (ticker)
    where
      sut.assetType = 'Stock'
      and
        sut.exchange = 'NYSE'
        or (sut.exchange = 'NASDAQ' and da.date >= '2002-10-01')
    -- Option 2: consider skipping tech stocks prior to 2001 crash, as this period might be
    -- so-so training data anyways
    -- UPDATE: leaning towards skipping all Nasdaq stocks prior October 2002
    --  Pros: simple
    --  Cons: arguably a cherrypicking strategy, making the trained model less robust.
    --        it takes away ~7% of the training data (but it might not be optimal training data)
  ), joined_pivot as (
    select *
    from pivoted_price
    join daily_subset as da
      on pivoted_price.date = da.date
  ), joined_with_volatility as (
    select *,
      -- index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 125) as volatility_6m,
      index_volatility(adjClose, SPY_adjClose, ticker, date, 250) as volatility_12m,
      -- index_volatility(adjClose, QQQ_adjClose, SPY_adjClose, exchange, ticker, date, 500) as volatility_24m,
      index_volatility(adjClose, SPY_adjClose, ticker, date, 750) as volatility_36m,
      sma(adjVolume, ticker, date, 250) as SMA_volume_12m,
      stddev(adjVolume) over (
        partition by ticker
        order by date
        rows between 250 preceding and current row
      ) / nullif(sma(adjVolume, ticker, date, 250), 0) as volume_volatility_12m,
    from joined_pivot
  ), with_index as (
    select * exclude (date_1)
    from joined_with_volatility
    -- Sample one day per quarter for each ticker
    -- 16th of the month is proposed since it will be the first
    -- safe_release_date after fiscal months that end on the 31st.
    -- Picking the months that are closest to _after_ safe_release_date.
    where
      day(date) >= 17
      and month(date) in (2, 5, 8, 11)
    qualify row_number() over (
      partition by ticker, year(date), quarter(date) order by date
    ) = 1
  ), quarterly_statements as (
    -- select relevant columns from statements an concat statementType and dataCode
    select
      ticker,
      date as fiscal_date,
      fiscal_date + 45 as safe_release_date, -- 45 days after fiscal date is the SEC deadline
      concat_ws('_', statementType, dataCode) as metrics,
      value
    from fundamentals.statements
    where
      quarter != 0 -- Ignore annual statements
  ), pivoted_statements as (
    -- pivot statements to a wide table
    pivot quarterly_statements on metrics
    using sum(value) -- Sum should be okay since there are no duplicates
  ), pivoted_with_meta as (
    select *
    from pivoted_statements
    left join fundamentals.meta as meta
      on pivoted_statements.ticker = upper(meta.ticker)
  ), joined as (
    select *
    from with_index as da
    asof inner join pivoted_with_meta as pivoted_statements -- inner join due to not interested in tickers without fundamentals
      on da.ticker = pivoted_statements.ticker and da.date >= pivoted_statements.safe_release_date
  )
  select * exclude(
    close,
    fiscal_date,
    safe_release_date, -- OBS: taking this out might make it harder debug if there could be lookahead bias.
    ticker_1,
    ticker_1_1,
    permaTicker,
    sicCode,
    companyWebsite,
    secFilingWebsite,
    statementLastUpdated,
    dailyLastUpdated,
    isActive,
  )
  from joined
)

