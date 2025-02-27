from os import environ
from dagster import (
    Definitions,
    asset,
    resource,
    AssetExecutionContext,
    Output,
    MetadataValue,
    TableColumn,
    TableSchema,
    define_asset_job,
    Field,
)
import numpy as np
import polars as pl
import duckdb
from pathlib import Path
from src.catboost_trainer import CatBoostTrainer

LOCAL = True if environ["APP_ENV"] == "dev" else False
print(f"Running in {'local' if LOCAL else 'production'} mode")

@resource(config_schema={"local": bool})
def duckdb_resource(init_context):
    local = init_context.resource_config["local"]
    if local:
        # Create a data directory if it doesn't exist
        data_dir = Path("src")
        data_dir.mkdir(exist_ok=True)
        return {
            "database": str(data_dir / "prod_copy_2024-11-08.db"),  # Local file-based database
            "schema": "main",
            "access_mode": "READ_WRITE",
        }
    else:
        return {
            "database": f"""md:{environ["APP_ENV"]}?motherduck_token={environ["MOTHERDUCK_TOKEN"]}""",
            "schema": "main",
        }


def duckdb_query_polars(sql: str, db_config: dict[str, str]) -> pl.DataFrame:
    """Query a DuckDB database and return the result as a Polars DataFrame """
    con = duckdb.connect(database=db_config["database"], read_only=True)
    try:
        df = con.query(sql).pl()
        return df
    finally:
        con.close()


def duckdb_query(sql: str, db_config: dict[str, str]) -> None:
    """Query a DuckDB database to create macros, views and tables"""
    con = duckdb.connect(database=db_config["database"], read_only=False)
    con.query(sql)
    con.close()


def write_table_duckdb(
    table_name: str, df: pl.DataFrame, db_config: dict[str, str]
) -> None:
    df.write_database(
        table_name=table_name,
        connection=f"duckdb:///{db_config['database']}",
        if_table_exists="replace",
    )


def query_duckdb_file(file: Path, db_config: dict[str, str]) -> None:
    sql = file.read_text()
    duckdb_query(sql, db_config)


@asset(required_resource_keys={"duckdb_config"})
def load_macros(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/macros.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[load_macros],
    config_schema={"cutoff_date": Field(str, is_required=False, default_value="current_date")}
)
def table_wide_statements(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config

    # Get the cutoff date from config, defaulting to current_date if not provided
    cutoff_date = context.op_config.get("cutoff_date", "current_date")

    # Read the SQL file
    with open("src/sql/1_wide_statements.sql", "r") as f:
        sql = f.read()

    # If cutoff_date is not a SQL function (like current_date), add quotes
    if cutoff_date != "current_date" and not cutoff_date.startswith("'") and not cutoff_date.endswith("'"):
        cutoff_date = f"'{cutoff_date}'"

    # Replace the parameter placeholder with the actual value
    modified_sql = sql.replace("where date <= $cutoff_date", f"where date <= {cutoff_date}")

    # Execute the modified SQL
    conn = duckdb.connect(database=db_config["database"], read_only=False)
    conn.execute(modified_sql)
    conn.close()


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[table_wide_statements],
)
def view_wide_with_daily_fundamentals(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/2_wide_with_daily_fundamentals.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[view_wide_with_daily_fundamentals]
)
def view_wide_with_combined_metrics(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/3_wide_with_combined_metrics.sql"), db_config)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[view_wide_with_combined_metrics]
)
def table_excess_returns(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/4_excess_returns.sql"), db_config)




@asset(
    required_resource_keys={"duckdb_config"},
    deps=[table_excess_returns]
)
def excess_returns(context: AssetExecutionContext) -> Output:
    db_config = context.resources.duckdb_config

    # Be explicit about the schema
    df = duckdb_query_polars("SELECT * FROM fundamentals.excess_returns", db_config)

    schema = [TableColumn(name=n, type=str(t)) for n, t in df.schema.items()]
    size_mb = df.estimated_size() / (1024 * 1024)

    return Output(
        value=df,
        metadata={
            "num_records": df.height,
            "dagster/row_count": MetadataValue.int(df.height),
            "dagster/column_schema": TableSchema(columns=schema),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "size_mb": MetadataValue.float(round(size_mb, 2)),
        }
    )


def _train_model_base(context: AssetExecutionContext, excess_returns: pl.DataFrame, pred_col: str) -> Output:
    """Base training function used by all prediction column variants."""
    db_config = context.resources.duckdb_config

    # Get the cutoff date from the context if it was provided
    cutoff_date = context.op_config.get("cutoff_date", None)

    conn = duckdb.connect(database=db_config["database"], read_only=True)
    seed = np.random.randint(0, 10)
    boost = CatBoostTrainer(
        conn=conn,
        df_excess_returns=excess_returns,
        pred_col=pred_col,
        seed=seed,
        cutoff_date=cutoff_date  # Pass the cutoff date to the trainer
    )
    boost.df_train_df()
    boost.split_train_test_pools()
    boost.model_init()
    boost.model_fit()
    df = boost.all_ticker_shaps()

    schema = [TableColumn(name=n, type=str(t)) for n, t in df.schema.items()]
    size_mb = df.estimated_size() / (1024 * 1024)

    return Output(
        value=df,
        metadata={
            "model_test_rmse": MetadataValue.float(float(round(boost.test_rmse, 4))),
            "model_seed": MetadataValue.int(seed),
            "model_timestamp": MetadataValue.text(boost.train_timestamp.isoformat()),
            "model_cutoff_date": MetadataValue.text(cutoff_date if cutoff_date else "current_date"),
            "model_best_iteration": MetadataValue.int(int(boost.model.best_iteration_)),
            "num_records": df.height,
            "row_count": MetadataValue.int(df.height),
            "column_schema": TableSchema(columns=schema),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "size_mb": MetadataValue.float(round(size_mb, 2)),
        }
    )


@asset(
    required_resource_keys={"duckdb_config"},
    config_schema={"cutoff_date": Field(str, is_required=False, default_value="current_date")}
)
def train_12m(context: AssetExecutionContext, excess_returns: pl.DataFrame) -> Output:
    return _train_model_base(context, excess_returns, "excess_return_ln_12m")

@asset(
    required_resource_keys={"duckdb_config"},
    config_schema={"cutoff_date": Field(str, is_required=False, default_value="current_date")}
)
def train_24m(context: AssetExecutionContext, excess_returns: pl.DataFrame) -> Output:
    return _train_model_base(context, excess_returns, "excess_return_ln_24m")

@asset(
    required_resource_keys={"duckdb_config"},
    config_schema={"cutoff_date": Field(str, is_required=False, default_value="current_date")}
)
def train_36m(context: AssetExecutionContext, excess_returns: pl.DataFrame) -> Output:
    return _train_model_base(context, excess_returns, "excess_return_ln_36m")


@asset
def concat_results(train_12m: pl.DataFrame, train_24m: pl.DataFrame, train_36m: pl.DataFrame) -> Output:
    df = pl.concat([train_12m, train_24m, train_36m])
    schema = [TableColumn(name=n, type=str(t)) for n, t in df.schema.items()]
    size_mb = df.estimated_size() / (1024 * 1024)

    return Output(
        value=df,
        metadata={
            "num_records": df.height,
            "dagster/row_count": MetadataValue.int(df.height),
            "dagster/column_schema": TableSchema(columns=schema),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "size_mb": MetadataValue.float(round(size_mb, 2)),
        }
    )


@asset(required_resource_keys={"duckdb_config"})
def insert_into_duckdb(context: AssetExecutionContext, concat_results: pl.DataFrame) -> None:
    db_config = context.resources.duckdb_config
    conn = duckdb.connect(database=db_config["database"], read_only=False)

    conn.query("""
        insert or replace into main.predictions
        select * from concat_results
    """)


@asset(
    required_resource_keys={"duckdb_config"},
    deps=[insert_into_duckdb]
)
def relevant_preds(context: AssetExecutionContext) -> None:
    db_config = context.resources.duckdb_config
    query_duckdb_file(Path("src/sql/relevant_preds.sql"), db_config)


########## Dagster Job ##########

catboost = define_asset_job(name="catboost")

defs = Definitions(
    assets=[
        load_macros,
        table_wide_statements,
        view_wide_with_daily_fundamentals,
        view_wide_with_combined_metrics,
        table_excess_returns,
        excess_returns,
        train_12m,
        train_24m,
        train_36m,
        concat_results,
        insert_into_duckdb,
        relevant_preds,
    ],
    resources={
        "duckdb_config": duckdb_resource.configured({"local": LOCAL})
    },
    jobs=[catboost],
)
