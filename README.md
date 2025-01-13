# stock-advisor

A simple stock advisor that predicts all listed US stocks' long term investment potential relative to the S&P 500 index, using CatBoost regression on tiingo.com's high quality financial data.

1. Downloads financial data from Tiingo's APIs and uploads them to Motherduck (Go-bases CLI that runs on a schedule on GH Actions).
2. Regularly runs data transformations + CatBoost training of models to present up-to-date results (Dagster runs on GH Actions on a scheduled interval, prediction results and SHAP values are stored in Motherduck).
3. Results and financial data are available in a Streamlit Cloud dashboard, which makes it easy to dive deep into and compare every stocks' predictions and SHAP value interpretations.

<img width="2952" alt="image" src="https://github.com/user-attachments/assets/a1c1c2d4-3995-4366-a0ea-1b79855b2216" />

[Tiingo](https://www.tiingo.com/about/pricing) has a restrictive license for its stock data, so there is _no_ data available in this repo
and the Motherduck database + the Streamlit dashboard in use is for my _private_ usage only. However, the idea is that if you bring your own Tiingo API key to this project,
you could get things up and running yourself pretty quickly.

> [!NOTE]
> This is a hobby project. My main focus is to get things up and running for myself, not that it works without effort for anybody else.
> But feel free to use as much as you'd like from it.

## TODOs/Roadmap

As this is early stage, tools and approaches might change along the way, but the plan in May 2024 looks something like this:

- [ ] Backfill [Motherduck](https://motherduck.com/) DB with all US stocks daily adjusted as listed in this file: https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
- [ ] Create daily batch job to updated with yesterday's data. Use Go and run job on GitHub Actions. Endpoint: https://api.tiingo.com/tiingo/daily/prices
- [ ] Subscribe to the Tiingo $10/month add-on for fundamentals, run backfill for all available stocks and schedule daily fundamentals ingest (Go+Github Actions).
- ~~[ ] Use [Malloy](https://docs.malloydata.dev/documentation/) for transformations.~~
  - UPDATE May 2024: Played around with Malloy a bit, and it is currently not expressive/flexible enough for all the transformations I had in mind,
    in particular it seemed to have little support for common time series operations. New plan is:
- [ ] Use DuckDB SQL for transformations. DuckDB SQL looks like an impressive improvement to standard SQL; I am optimistic
  it can provide enough flexibility, reusability and composability to not be frustrating to work with. Goal is to move
  reusable logic into `MACRO`s and `FUNCTION`s, and run unit tests on logic via Pytest.
- [ ] Create visualizations, tables, dashboards and notebooks in [Observable Framework](https://observablehq.com/framework/).
- [ ] Use [Malloy](https://docs.malloydata.dev/documentation/) for the semantic layer/metrics definitions, which will be used by the Observable Framework front-end.
- [ ] Orchestrate statistical and machine learning models with [dagster](https://dagster.io/) running on Github Actions and save results to Motherduck DB.

