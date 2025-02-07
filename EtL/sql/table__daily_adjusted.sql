create table if not exists daily_adjusted (
  date DATE,
  close DECIMAL,
  adjClose DECIMAL,
  adjVolume UBIGINT,
  ticker VARCHAR,
  primary key (ticker, date)
);
