# init-db

Python classes and functions for initializing a DuckDB/Motherduck database with data from the Tiingo API. Currently, the code only supports the [end-of-day](https://www.tiingo.com/documentation/end-of-day) API, but I'd guess one could reuse most of this code for any other endpoint with smaller modifications.

Personally, I use the `init.ipynb` notebook to run the fetch and ingest jobs, but you could easily compose the same functionality into a `.py`-file if you prefer that.
