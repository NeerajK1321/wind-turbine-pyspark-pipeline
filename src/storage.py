from pathlib import Path
import duckdb
from pyspark.sql import DataFrame


def write_tables(
    cleaned: DataFrame,
    summary: DataFrame,
    anomalies: DataFrame,
    database_path: str,
) -> None:
    """Persist Spark DataFrames into a small analytical database for the POC."""
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(database_path) as con:
        # Registering the Spark DataFrames through pandas keeps this storage layer
        # intentionally simple for a take-home POC. The processing remains in Spark.
        con.register("cleaned_view", cleaned.toPandas())
        con.register("summary_view", summary.toPandas())
        con.register("anomalies_view", anomalies.toPandas())

        con.execute("CREATE OR REPLACE TABLE cleaned_measurements AS SELECT * FROM cleaned_view")
        con.execute("CREATE OR REPLACE TABLE turbine_daily_summary AS SELECT * FROM summary_view")
        con.execute("CREATE OR REPLACE TABLE turbine_anomalies AS SELECT * FROM anomalies_view")
