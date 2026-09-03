import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.pipeline import validate_and_clean, calculate_summary, identify_anomalies


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("wind-turbine-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_missing_value_is_imputed_with_turbine_median(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0, 180, 2.0),
        ("2022-03-01 01:00:00", 1, None, 181, 2.2),
        ("2022-03-01 02:00:00", 1, 12.0, 182, 2.4),
    ]
    df = spark.createDataFrame(
        rows,
        ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"],
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    cleaned = validate_and_clean(df)
    value = cleaned.filter(F.col("timestamp") == F.to_timestamp(F.lit("2022-03-01 01:00:00"))).first()["wind_speed"]
    assert value == pytest.approx(10.0)


def test_duplicate_measurement_is_removed(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0, 180, 2.0),
        ("2022-03-01 00:00:00", 1, 10.0, 180, 2.0),
    ]
    df = spark.createDataFrame(
        rows, ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    assert validate_and_clean(df).count() == 1


def test_summary_contains_required_statistics(spark):
    rows = [
        ("2022-03-01 00:00:00", 1, 10.0, 180, 2.0),
        ("2022-03-01 01:00:00", 1, 10.0, 180, 4.0),
    ]
    df = spark.createDataFrame(
        rows, ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]
    ).withColumn("timestamp", F.to_timestamp("timestamp"))

    summary = calculate_summary(df).first()
    assert summary["min_power_output_mw"] == 2.0
    assert summary["max_power_output_mw"] == 4.0
    assert summary["avg_power_output_mw"] == 3.0


def test_anomaly_rule_uses_two_standard_deviations(spark):
    rows = [
        (1, 3.0, 1.0, 10),
        (2, 3.0, 1.0, 10),
        (3, 3.0, 1.0, 10),
        (4, 7.0, 1.0, 10),
        (5, 3.0, 1.0, 10),
    ]
    summary = spark.createDataFrame(
        rows,
        ["turbine_id", "avg_power_output_mw", "power_output_stddev_mw", "measurement_count"],
    )
    result = identify_anomalies(summary)
    anomaly_ids = [r["turbine_id"] for r in result.filter("is_anomaly").collect()]
    assert anomaly_ids == [4]
