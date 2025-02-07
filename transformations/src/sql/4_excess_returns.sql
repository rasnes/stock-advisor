create or replace table fundamentals.excess_returns as (
with excess_returns as (
   select *,
      future_return(SPY_adjClose, ticker, date, 2) as SPY_return_6m,
      future_return(adjClose, ticker, date, 2) as adjClose_return_6m,

      future_return(SPY_adjClose, ticker, date, 4) as SPY_return_12m,
      future_return(adjClose, ticker, date, 4) as adjClose_return_12m,

      future_return(SPY_adjClose, ticker, date, 8) as SPY_return_24m,
      future_return(adjClose, ticker, date, 8) as adjClose_return_24m,

      future_return(SPY_adjClose, ticker, date, 12) as SPY_return_36m,
      future_return(adjClose, ticker, date, 12) as adjClose_return_36m,

      -- Original excess returns
      excess_return(adjClose_return_6m, SPY_return_6m) as excess_return_ln_6m,
      excess_return(adjClose_return_12m, SPY_return_12m) as excess_return_ln_12m,
      excess_return(adjClose_return_24m, SPY_return_24m) as excess_return_ln_24m,
      excess_return(adjClose_return_36m, SPY_return_36m) as excess_return_ln_36m,


      -- Simplified relative SMA development metrics
      -- relative_sma_development(SMA_6m, QQQ_SMA_6m, SPY_SMA_6m, ticker, date) as relative_sma_development_6m,
      relative_sma_development(SMA_12m, SPY_SMA_12m, ticker, date) as relative_sma_development_12m,
      -- relative_sma_development(SMA_24m, QQQ_SMA_24m, SPY_SMA_24m, ticker, date) as relative_sma_development_24m,
      relative_sma_development(SMA_36m, SPY_SMA_36m, ticker, date) as relative_sma_development_36m,

   from fundamentals.wide_with_combined_metrics
   order by ticker, date desc
)
   from excess_returns
   select *
   exclude (
      SPY_adjClose, adjClose,
      -- SMA_6m, SMA_24m,
      SMA_12m, SMA_36m,
      -- QQQ_SMA_6m, SPY_SMA_6m,
      SPY_SMA_12m,
      -- QQQ_SMA_24m, SPY_SMA_24m,
      SPY_SMA_36m,
      SPY_return_6m, adjClose_return_6m,
      SPY_return_12m, adjClose_return_12m,
      SPY_return_24m, adjClose_return_24m,
      SPY_return_36m, adjClose_return_36m,
      -- Columns deemed unnecessary for the final dataset
      balanceSheet_sharesBasic,
      overview_shareFactor,
      balanceSheet_deferredRev,
      balanceSheet_taxLiabilities,
      balanceSheet_acctPay,
      balanceSheet_deposits,
      incomeStatement_eps,  -- keep epsDil instead
      incomeStatement_shareswa,  -- keep shareswaDil instead
      incomeStatement_shareswaDil,
      incomeStatement_netinc,  -- keep netIncComStock instead
      isADR,
      balanceSheet_totalAssets,
      balanceSheet_taxAssets,
      incomeStatement_netIncDiscOps,
      incomeStatement_prefDVDs,
      incomeStatement_nonControllingInterests,
      incomeStatement_ebt,
      balanceSheet_assetsCurrent,
      cashFlow_ncfx,
      cashFlow_businessAcqDisposals,
      balanceSheet_investmentsCurrent,
      incomeStatement_consolidatedIncome,  -- use netIncComStock instead
      marketCap, -- use enterpriseVal instead
      balanceSheet_accoci,
      reportingCurrency,
      adjVolume,
      overview_piotroskiFScore,
      cashFlow_ncf,
      cashFlow_ncff,
      -- New, more speculative columns to omit:
      cashFlow_capex,
      cashFlow_issrepayDebt,
      balanceSheet_liabilitiesNonCurrent,
      incomeStatement_costRev,
      incomeStatement_ebit,
      incomeStatement_intexp,
      cashFlow_ncfo,
      cashFlow_ncfi,
      trailingPEG1Y,
   )
);

create index idx_excess_returns_ticker_date
on fundamentals.excess_returns(ticker, date);
