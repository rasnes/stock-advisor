# stock-advisor

A simple stock advisor that predicts all listed US stocks' long term investment potential relative to the S&P 500 index, using CatBoost regression on tiingo.com's high quality financial data.

1. Downloads financial data from Tiingo's APIs and uploads them to Motherduck (Go-bases CLI that runs on a schedule on GH Actions).
2. Regularly runs data transformations + CatBoost training of models to present up-to-date results (Dagster runs on GH Actions on a scheduled interval, prediction results and SHAP values are stored in Motherduck).
3. Results and financial data are available in a Streamlit Cloud dashboard, which makes it easy to dive deep into and compare every stocks' predictions and SHAP value interpretations.

See the blog post series at my personal site for more about this product: https://rasmusnes.com/posts/stock-advisor-intro/

<img width="2952" alt="image" src="https://github.com/user-attachments/assets/a1c1c2d4-3995-4366-a0ea-1b79855b2216" />

[Tiingo](https://www.tiingo.com/about/pricing) has a restrictive license for its stock data, so there is _no_ data available in this repo
and the Motherduck database + the Streamlit dashboard in use is for my _private_ usage only. However, the idea is that if you bring your own Tiingo API key to this project,
you could get things up and running yourself pretty quickly.

> [!NOTE]
> This is a hobby project. My main focus is to get things up and running for myself, not that it works without effort for anybody else.
> But feel free to use as much as you'd like from it.

## Status

For me this project is more or less complete, and the pipelines running in Github Actions has yet to fail on me. I regularly go into the Streamlit Dashboard to evaluate stocks, usually in mix with content from from other sources like Yahoo Finance (free) and Motley Fool (paid).

However, it may be that I'll add some more dashboards or predictors to the models, but this might go largely undocumented.


### TODOs/Roadmap

January 2025: Even though I did not end up using many of the things below (most notably Malloy and Observable Framework), I decided to keep them there for transparency.

- [x] Backfill [Motherduck](https://motherduck.com/) DB with all US stocks daily adjusted as listed in this file: https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
- [x] Create daily batch job to updated with yesterday's data. Use Go and run job on GitHub Actions. Endpoint: https://api.tiingo.com/tiingo/daily/prices
- [x] Subscribe to the Tiingo $10/month add-on for fundamentals, run backfill for all available stocks and schedule daily fundamentals ingest (Go+Github Actions).
- [ ] ~~Use [Malloy](https://docs.malloydata.dev/documentation/) for transformations.~~
  - UPDATE May 2024: Played around with Malloy a bit, and it is currently not expressive/flexible enough for all the transformations I had in mind,
    in particular it seemed to have little support for common time series operations. New plan is:
- [x] Use DuckDB SQL for transformations. DuckDB SQL looks like an impressive improvement to standard SQL; I am optimistic
  it can provide enough flexibility, reusability and composability to not be frustrating to work with. Goal is to move
  reusable logic into `MACRO`s and `FUNCTION`s, and run unit tests on logic via Pytest.
- [ ] ~~Create visualizations, tables, dashboards and notebooks in [Observable Framework](https://observablehq.com/framework/).~~
  - UPDATE: I tried [Observable Framework](https://observablehq.com/framework/) a bit, and even though I liked some parts of it I landed on it not being an ideal fit in this case. Primarily because I found it to be significantly less complex setup to just fetch the data used in the visualization layer directly from Motherduck, as opposed to loading all data into the front-end itself on deploy time (Framework data loader). Configuring a good data loader setup is quite a bit of overhead, and fetching data directly from Motherduck from the _front end_ exposes tokens in the browser which is not a good idea even though the static site would have been non-public. In addition, I just found the devex for developing tables and charts exactly how I want them much better in Streamlit than in Observable Framework (very limited help from the IDE in markdown docuements, for example, was a source of frustration).
- [ ] ~~Use [Malloy](https://docs.malloydata.dev/documentation/) for the semantic layer/metrics definitions, which will be used by the Observable Framework front-end.~~
  - UPDATE: for the as-of-now simple transformations needed for this project, a dedicated semantic layer was found excessive and unnecessary.
- [x] Orchestrate statistical and machine learning models with [dagster](https://dagster.io/) running on Github Actions and save results to Motherduck DB.
  - UPDATE: After trying several models, I ended up just using one model, `CatBoostUncertaintyRegressor`, which has high-quality predictions, includes uncertainty intervals, and is easy to work with for both missing values and categorical values.
