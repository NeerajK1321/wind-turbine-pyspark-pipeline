from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from .storage import write_tables


SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("turbine_id", IntegerType(), nullable=False),
        StructField("wind_speed", DoubleType(), nullable=True),
        StructField("wind_direction", IntegerType(), nullable=True),
        StructField("power_output", DoubleType(), nullable=True),
    ]
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("wind-turbine-pipeline")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_input(spark: SparkSession, input_dir: str) -> DataFrame:
    paths = str(Path(input_dir) / "data_group_*.csv")
    return (
        spark.read
        .option("header", True)
        .schema(SCHEMA)
        .csv(paths)
    )


def validate_and_clean(df: DataFrame) -> DataFrame:
    # Keep the transformation deterministic: duplicate measurements are removed
    # before imputation so a repeated source row cannot influence the median.
    df = df.dropDuplicates(["timestamp", "turbine_id"])

    df = df.filter(
        (F.col("turbine_id").between(1, 15))
        & ((F.col("wind_speed").isNull()) | (F.col("wind_speed") >= 0))
        & ((F.col("wind_direction").isNull()) | F.col("wind_direction").between(0, 359))
        & ((F.col("power_output").isNull()) | (F.col("power_output") >= 0))
    )

    # Turbine-specific median is more appropriate than a fleet median because
    # turbines may have different normal operating levels.
    median_window = Window.partitionBy("turbine_id")

    for column in ("wind_speed", "wind_direction", "power_output"):
        median = F.percentile_approx(F.col(column), 0.5).over(median_window)
        df = df.withColumn(f"{column}_median", median)

    # IQR outlier rule, calculated independently for each turbine and field.
    for column in ("wind_speed", "wind_direction", "power_output"):
        q1 = F.percentile_approx(F.col(column), 0.25).over(median_window)
        q3 = F.percentile_approx(F.col(column), 0.75).over(median_window)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        df = df.withColumn(
            column,
            F.when(
                F.col(column).isNull()
                | (F.col(column) < lower)
                | (F.col(column) > upper),
                F.col(f"{column}_median"),
            ).otherwise(F.col(column)),
        )

    return df.select(
        "timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"
    )


def calculate_summary(cleaned: DataFrame) -> DataFrame:
    return (
        cleaned.groupBy("turbine_id")
        .agg(
            F.min("power_output").alias("min_power_output_mw"),
            F.max("power_output").alias("max_power_output_mw"),
            F.avg("power_output").alias("avg_power_output_mw"),
            F.stddev_samp("power_output").alias("power_output_stddev_mw"),
            F.count("*").alias("measurement_count"),
            F.min("timestamp").alias("period_start"),
            F.max("timestamp").alias("period_end"),
        )
        .orderBy("turbine_id")
    )


def identify_anomalies(summary: DataFrame) -> DataFrame:
    fleet_stats = summary.agg(
        F.avg("avg_power_output_mw").alias("fleet_mean_avg_mw"),
        F.stddev_samp("avg_power_output_mw").alias("fleet_stddev_avg_mw"),
    )

    return (
        summary.crossJoin(fleet_stats)
        .withColumn(
            "lower_threshold_mw",
            F.col("fleet_mean_avg_mw") - 2 * F.col("fleet_stddev_avg_mw"),
        )
        .withColumn(
            "upper_threshold_mw",
            F.col("fleet_mean_avg_mw") + 2 * F.col("fleet_stddev_avg_mw"),
        )
        .withColumn(
            "is_anomaly",
            (F.col("avg_power_output_mw") < F.col("lower_threshold_mw"))
            | (F.col("avg_power_output_mw") > F.col("upper_threshold_mw")),
        )
        .select(
            "turbine_id",
            "avg_power_output_mw",
            "fleet_mean_avg_mw",
            "fleet_stddev_avg_mw",
            "lower_threshold_mw",
            "upper_threshold_mw",
            "is_anomaly",
        )
        .orderBy("turbine_id")
    )


def run_pipeline(input_dir: str, output_db: str) -> Tuple[DataFrame, DataFrame, DataFrame]:
    spark = create_spark()
    try:
        raw = read_input(spark, input_dir)
        cleaned = validate_and_clean(raw).cache()
        summary = calculate_summary(cleaned).cache()
        anomalies = identify_anomalies(summary).cache()

        write_tables(cleaned, summary, anomalies, output_db)

        print("Pipeline completed successfully.")
        print(f"Cleaned rows: {cleaned.count()}")
        print(f"Turbines summarised: {summary.count()}")
        print(f"Anomalies: {anomalies.filter(F.col('is_anomaly')).count()}")

        return cleaned, summary, anomalies
    finally:
        spark.stop()
