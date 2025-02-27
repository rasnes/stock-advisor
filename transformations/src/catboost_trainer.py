from os import environ
import duckdb
import pandas as pd
import numpy as np
import polars as pl
from typing import Set
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from catboost import CatBoostRegressor, Pool, EFstrType
import matplotlib.pyplot as plt
import shap
from datetime import datetime
import re

class CatBoostTrainer:
    """Prepares data from DuckDB for training."""

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        df_excess_returns: pl.DataFrame,
        pred_col: str,
        seed: int,
        cutoff_date: str | None = None,
    ) -> None:
        """Init with DuckDB connection."""
        self.conn: duckdb.DuckDBPyConnection = conn
        self.pred_col: str = pred_col
        self.seed: int = seed
        self.cutoff_date: str | None = cutoff_date
        self._all_exclude_cols: Set[str] = {
            "name",
            "excess_return_ln_6m",
            "excess_return_ln_12m",
            "excess_return_ln_24m",
            "excess_return_ln_36m",
        }
        self._model_exclude_cols: Set[str] = self._all_exclude_cols | {"ticker", "date"}
        self.categorical_features_indices: np.ndarray
        self.df_preds: pd.DataFrame
        self.X_test: pd.DataFrame
        self.y_test: pd.Series
        self.test_tickers: pd.Series
        self.train_pool: Pool
        self.eval_pool: Pool
        self.test_pool: Pool
        self.model: CatBoostRegressor
        self.shap_values: np.ndarray = np.array([])
        self.test_rmse: np.ndarray
        self.train_timestamp: datetime
        self.training_iterations = {"dev": 200, "test": 1, "prod": 2000}.get(
            environ["APP_ENV"], 2000
        )

        # Convert all Decimal types to Float64 and replace missing values in string columns
        self.df_excess_returns: pl.DataFrame = df_excess_returns.with_columns(
            [
                # Handle Decimal to Float64 conversion
                *[
                    pl.col(col).cast(pl.Float64)
                    for col in df_excess_returns.columns
                    if df_excess_returns.schema[col] == pl.Decimal
                ],
                # Replace null values with "None" string in string columns
                *[
                    pl.col(col).fill_null("None")
                    for col in df_excess_returns.columns
                    if df_excess_returns.schema[col] == pl.String
                ],
            ]
        )

    def db_train_df(self) -> None:
        """Get training DataFrame excluding specified cols except prediction col.

        Returns: pd.DataFrame: Training data with specified cols excluded
        """
        exclude_cols = self._all_exclude_cols.difference({self.pred_col})
        exclude_cols_str = ",\n".join(exclude_cols)

        self.df_preds = self.conn.query(f"""
            select * exclude({exclude_cols_str})
            from fundamentals.excess_returns
            where {self.pred_col} is not null
        """).df()
        self.df_preds = self.df_preds

        size_bytes = self.df_preds.memory_usage(deep=True).sum()
        size_mb = size_bytes / (1024 * 1024)
        print(f"DataFrame size: {size_mb:.2f} MB")

    def df_train_df(self) -> None:
        """Get training DataFrame from existing DataFrame.

        Returns: pd.DataFrame: Training data subset
        """
        if self.df_excess_returns is None:
            raise ValueError("No data loaded. Run db_excess_returns() first.")

        exclude_cols = self._all_exclude_cols.difference({self.pred_col})
        self.df_preds = self.df_excess_returns.to_pandas().drop(
            columns=list(exclude_cols)
        )
        self.df_preds = self.df_preds.dropna(subset=[self.pred_col])
        self.df_preds = self.df_preds

        size_bytes = self.df_preds.memory_usage(deep=True).sum()
        size_mb = size_bytes / (1024 * 1024)
        print(f"DataFrame size: {size_mb:.2f} MB")

    def split_train_test_pools(self, test_size=0.03, val_size=0.05) -> None:
        # First split the full dataset including ticker
        X_temp_full, X_test_full, y_temp, self.y_test = train_test_split(
            self.df_preds.drop(self.pred_col, axis=1),  # Keep ticker here
            self.df_preds[self.pred_col],
            test_size=test_size,
            random_state=self.seed,
        )
        drop_cols = ["ticker", "date"]

        # Store X_test with ticker for later use
        self.X_test = X_test_full.drop(drop_cols, axis=1)

        # Create version without ticker for model training
        X_temp = X_temp_full.drop(drop_cols, axis=1)

        # Get categorical features indices after ticker is removed
        self.categorical_features_indices = np.where(self.X_test.dtypes != float)[0]

        X_train, X_val, y_train, y_val = train_test_split(
            X_temp, y_temp, train_size=(1 - val_size), random_state=self.seed
        )

        self.train_pool = Pool(
            X_train, y_train, cat_features=self.categorical_features_indices
        )
        self.eval_pool = Pool(
            X_val, y_val, cat_features=self.categorical_features_indices
        )
        self.test_pool = Pool(
            self.X_test, self.y_test, cat_features=self.categorical_features_indices
        )

    def model_init(self) -> None:
        # TODO maybe: include model for excess_return_ln_6m, even though it overfits
        if self.pred_col == "excess_return_ln_6m":
            # To mitigate overfitting
            model_size_reg = 0.1
            l2_leaf_reg = 20
            subsample = 0.8
            min_data_in_leaf = 15
        else:
            l2_leaf_reg = 15
            model_size_reg = 0.08
            subsample = 0.9
            min_data_in_leaf = 10

        depth = 6
        iterations = self.training_iterations
        learning_rate = 0.15
        early_stopping_rounds = 25

        self.model = CatBoostRegressor(
            iterations=iterations,
            learning_rate=learning_rate,
            depth=depth,
            l2_leaf_reg=l2_leaf_reg,
            min_data_in_leaf=min_data_in_leaf,
            model_size_reg=model_size_reg,
            subsample=subsample,
            loss_function="RMSEWithUncertainty",
            early_stopping_rounds=early_stopping_rounds,
            verbose=False,
            random_seed=self.seed,
        )

    def model_fit(self) -> None:
        self.model.fit(
            self.train_pool,
            eval_set=self.eval_pool,
            use_best_model=True,
            verbose=100,
        )

        # Get test set predictions
        test_preds = self.model.predict(self.test_pool)
        mean_preds = test_preds[:, 0]  # Get only mean predictions, not variance
        self.test_rmse = np.sqrt(
            mean_squared_error(self.test_pool.get_label(), mean_preds)
        )
        self.train_timestamp = datetime.now()

        print("\nOutcome variable:", self.pred_col)
        print("Best iteration:", self.model.get_best_iteration())
        print(
            "Validation RMSE:",
            self.model.get_best_score()["validation"]["RMSEWithUncertainty"],
        )
        print(f"Holdout test set RMSE: {self.test_rmse:.6f}")

    def ticker_preds(self, ticker: str, since: str = "2019-01-01") -> pd.DataFrame:
        """Get predictions for a single ticker."""
        ticker = ticker.upper()
        exclude_cols = ", ".join(self._model_exclude_cols)

        df_preds = self.conn.query(f"""
            select * exclude({exclude_cols})
            from fundamentals.excess_returns
            where ticker = '{ticker}' and date > '{since}'
        """).df()

        df_actual = self.conn.query(f"""
            select date, {self.pred_col}
            from fundamentals.excess_returns
            where ticker = '{ticker}' and date > '{since}'
        """).df()

        preds = pd.DataFrame(self.model.predict(df_preds), columns=["mean", "var"])
        df_out = (
            pd.concat(
                [
                    preds,
                    df_preds.reset_index(drop=True),
                    df_actual.reset_index(drop=True),
                ],
                axis=1,
            )
            .assign(
                predicted_excess_return=lambda df: np.exp(df["mean"]),
                predicted_std=lambda df: np.sqrt(np.exp(2 * df["mean"]) * df["var"]),
                actual_excess_return=lambda df: np.exp(df[self.pred_col]),
            )
            .filter(
                [
                    "predicted_excess_return",
                    "predicted_std",
                    "ticker",
                    "date",
                    "actual_excess_return",
                ]
            )
            .sort_values("date", ascending=False)
        )
        return df_out

    def feature_importance_plot(self) -> None:
        # TODO: consider making this plot denser in y-axis (smaller bars)
        feature_imp_df = pd.DataFrame(
            {
                "feature": self.X_test.columns,
                "importance": self.model.feature_importances_,
            }
        )

        # Sort by importance descending
        feature_imp_df = feature_imp_df.sort_values("importance", ascending=True)

        # Plot horizontal bar chart
        plt.figure(figsize=(10, max(8, len(feature_imp_df) * 0.3)))
        plt.barh(feature_imp_df["feature"], feature_imp_df["importance"])
        plt.title("Feature Importances")
        plt.xlabel("Importance")
        plt.tight_layout()
        plt.show()

    def get_shap_values(self) -> None:
        shap_values = self.model.get_feature_importance(
            data=self.test_pool,
            type=EFstrType.ShapValues,
            shap_mode="UsePreCalc",
            verbose=False,
        )

        # Remove variance column
        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, 0, :]

        self.feature_names = self.X_test.columns
        self.shap_values = shap_values

    def shap_beeswarm(self) -> None:
        if len(self.shap_values) == 0:
            self.get_shap_values()

        # Remove the bias term (last column)
        shap_values = self.shap_values[:, :-1]

        # Print shapes for debugging
        print("SHAP values shape:", shap_values.shape)
        print("X_eval shape:", self.X_test.values.shape)
        print("Number of feature names:", len(self.feature_names))

        explanation = shap.Explanation(
            values=shap_values,
            base_values=np.zeros(len(self.X_test)),
            data=self.X_test.values,
            feature_names=self.feature_names,
        )

        shap.plots.beeswarm(explanation, max_display=40)

    def all_ticker_shaps(self, since: str | None = None) -> pl.DataFrame:
        """Generate shap values and predictions FOR all tickers.
        Results to be exported to DuckDB in a subsequent step in pipeline."""

        # Load data using Polars
        df_excess = self.df_excess_returns

        # Filter by date if provided
        if since is not None:
            df_excess = df_excess.filter(pl.col("date") >= since)
            if df_excess.height == 0:
                raise ValueError(f"No data found since {since}")
        else:
            # If no date, get latest record for each ticker
            df_excess = (
                df_excess.sort("date", descending=True).group_by("ticker").head(1)
            )

        # Convert to pandas for compatibility with existing code
        ticker_data = df_excess.to_pandas()

        # Store metadata
        dates = ticker_data["date"]
        tickers = ticker_data["ticker"]
        actual_values = ticker_data[self.pred_col]

        # Prepare features
        X_ticker = ticker_data.drop(
            columns=list(self._model_exclude_cols | {self.pred_col})
        )
        X_ticker = X_ticker
        y_ticker = ticker_data[self.pred_col]

        # Create pool and get predictions
        ticker_pool = Pool(
            X_ticker, y_ticker, cat_features=self.categorical_features_indices
        )
        predictions = self.model.predict(ticker_pool)
        mean_preds = predictions[:, 0]
        var_preds = predictions[:, 1]

        # Calculate SHAP values
        shap_values = self.model.get_feature_importance(
            data=ticker_pool,
            type=EFstrType.ShapValues,
            shap_mode="UsePreCalc",
            verbose=False,
        )

        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, 0, :]

        print(shap_values.shape)

        ## Create results for export

        # Calculate repeated indices for features
        n_samples = len(dates)
        n_features = len(X_ticker.columns)
        sample_idx = np.repeat(np.arange(n_samples), n_features)

        # Convert Pandas Series to arrays before creating DataFrame
        dates_array = dates.to_numpy()
        tickers_array = tickers.to_numpy()
        actual_values_array = actual_values.to_numpy()

        # Simply convert all feature values to strings
        feature_values = X_ticker.values.flatten().astype(str)

        print("creating results df")

        # Create base DataFrame
        results = pl.DataFrame(
            {
                "date": pl.Series(dates_array[sample_idx], dtype=pl.Date),
                "ticker": pl.Series(tickers_array[sample_idx], dtype=pl.Utf8),
                "feature": pl.Series(
                    np.tile(X_ticker.columns, n_samples), dtype=pl.Utf8
                ),
                "shap_value": pl.Series(
                    shap_values[:, :-1].flatten(), dtype=pl.Float64
                ),
                "feature_value": pl.Series(
                    feature_values, dtype=pl.Utf8
                ),  # All as strings
                "bias": pl.Series(
                    np.repeat(shap_values[:, -1], n_features), dtype=pl.Float64
                ),
                "predicted_value_log": pl.Series(
                    np.repeat(mean_preds, n_features), dtype=pl.Float64
                ),
                "actual_value_log": pl.Series(
                    np.repeat(actual_values_array, n_features), dtype=pl.Float64
                ),
            }
        )

        # Before the with_columns call, determine the date to use
        # Then in with_columns, just use the pre-computed value
        if self.cutoff_date is not None and re.match(r"^'?\d{4}-\d{2}-\d{2}'?$", self.cutoff_date):
            date_str = re.search(r"(\d{4}-\d{2}-\d{2})", self.cutoff_date).group(1)
            trained_date = pl.lit(date_str).cast(pl.Date)
        else:
            trained_date = pl.lit(self.train_timestamp.date().isoformat()).cast(pl.Date)

        results = (
            results
            # First add var_preds as a column
            .with_columns(pl.Series("var_preds", np.repeat(var_preds, n_features)))
            # Then do all transformations using pure Polars
            .with_columns(
                [
                    pl.col("predicted_value_log").exp().alias("predicted_value"),
                    (pl.col("predicted_value_log").exp() * 2 * pl.col("var_preds"))
                    .sqrt()
                    .alias("predicted_std"),
                    pl.when(pl.col("actual_value_log").is_not_null())
                    .then(pl.col("actual_value_log").exp())
                    .otherwise(None)
                    .alias("actual_value"),
                    pl.col("shap_value").abs().alias("abs_shap"),
                    pl.lit(self.pred_col).alias("pred_col"),
                    pl.lit(self.train_timestamp.isoformat())
                    .cast(pl.Datetime)
                    .alias("trained_at"),
                    trained_date.alias("trained_date"),
                ]
            )
            .drop("var_preds")  # Remove temporary column
            .sort(["date", "ticker", "abs_shap"], descending=[False, False, True])
            .drop("abs_shap")
        )

        return results

    def ticker_shap(self, ticker: str, since: str | None = None) -> pd.DataFrame:
        ticker = ticker.upper()

        # Use df_excess_returns instead of df_preds to get all dates
        df_excess = self.df_excess_returns.to_pandas()
        ticker_data = df_excess[df_excess["ticker"] == ticker].copy()
        if len(ticker_data) == 0:
            raise ValueError(f"Ticker {ticker} not found in dataset")

        # Filter by date if since is provided
        if since is not None:
            ticker_data = ticker_data[ticker_data["date"] >= since]
            if len(ticker_data) == 0:
                raise ValueError(f"No data found for {ticker} since {since}")
        else:
            # If no date provided, just get the latest record
            ticker_data = ticker_data.sort_values("date", ascending=False).head(1)

        # Store date and ticker before dropping them for the model
        dates = ticker_data["date"]
        tickers = ticker_data["ticker"]
        actual_values = ticker_data[self.pred_col]

        # Drop columns not used in model
        X_ticker = ticker_data.drop(
            columns=list(self._model_exclude_cols | {self.pred_col})
        )
        X_ticker = X_ticker
        y_ticker = ticker_data[self.pred_col]

        ticker_pool = Pool(
            X_ticker, y_ticker, cat_features=self.categorical_features_indices
        )

        # Get predictions
        predictions = self.model.predict(ticker_pool)
        mean_preds = predictions[:, 0]  # Get mean predictions
        var_preds = predictions[:, 1]  # Get variance predictions

        # Calculate SHAP values
        shap_values = self.model.get_feature_importance(
            data=ticker_pool,
            type=EFstrType.ShapValues,
            shap_mode="UsePreCalc",
            verbose=False,
        )

        # Remove variance column if present
        if len(shap_values.shape) == 3:
            shap_values = shap_values[:, 0, :]

        print(shap_values.shape)

        # Create list to store results for each date
        results = []

        for i in range(len(dates)):
            # Create DataFrame for this date
            row_data = pd.DataFrame(
                {
                    "Date": dates.iloc[i],
                    "Ticker": tickers.iloc[i],
                    "Feature": X_ticker.columns,
                    "SHAP Value": shap_values[i, :-1],
                    "Feature Value": X_ticker.iloc[i].values,
                    "Bias": shap_values[i, -1],
                    "Predicted Value (log)": mean_preds[i],
                    "Predicted Value": np.exp(mean_preds[i]),
                    "Predicted Std": np.sqrt(np.exp(2 * mean_preds[i]) * var_preds[i]),
                    "Actual Value (log)": actual_values.iloc[i],
                    "Actual Value": np.exp(actual_values.iloc[i])
                    if pd.notnull(actual_values.iloc[i])
                    else None,
                }
            )

            # Sort by absolute SHAP values
            row_data["Abs SHAP"] = row_data["SHAP Value"].abs()
            row_data = row_data.sort_values("Abs SHAP", ascending=False)
            row_data = row_data.drop("Abs SHAP", axis=1)

            results.append(row_data)

        # Combine all results
        final_df = pd.concat(results, axis=0)

        # Print summary for the most recent date
        latest_date = dates.max()
        latest_data = final_df[final_df["Date"] == latest_date].iloc[0]
        print(f"\nMost recent prediction ({latest_date}):")
        print(f"Bias (expected value): {latest_data['Bias']:0.4f}")
        print(
            f"Predicted value (log scale): {latest_data['Predicted Value (log)']:0.4f}"
        )
        print(
            f"Predicted value (original scale): {latest_data['Predicted Value']:0.4f}"
        )
        print(f"Predicted std: {latest_data['Predicted Std']:0.4f}")
        if pd.notnull(latest_data["Actual Value (log)"]):
            print(
                f"Actual target value (log scale): {latest_data['Actual Value (log)']:0.4f}"
            )
            print(
                f"Actual target value (original scale): {latest_data['Actual Value']:0.4f}"
            )
        else:
            print("Actual target value: Not yet available")
        print()

        return final_df
