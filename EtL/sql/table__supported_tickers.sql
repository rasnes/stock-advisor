-- TODO: check that this table as write-truncate semantics
create table if not exists supported_tickers (
  ticker VARCHAR,
  exchange VARCHAR,
  assetType VARCHAR,
  priceCurrency VARCHAR,
  startDate DATE,
  endDate DATE
);