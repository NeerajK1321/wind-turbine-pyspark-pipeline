# Wind Turbine Data Processing Pipeline

Proof-of-concept data engineering take-home for processing daily CSV measurements from 15 wind turbines.

## What it does

1. Reads the three turbine CSV groups with PySpark.
2. Validates schema and basic domain constraints.
3. Removes exact duplicate measurements.
4. Treats missing numeric measurements with turbine-level median imputation.
5. Detects statistical outliers with a per-turbine IQR rule and imputes them with the turbine median.
6. Calculates 24-hour minimum, maximum and average power output for each turbine.
7. Flags turbines whose 24-hour average is outside two standard deviations of the fleet's 24-hour turbine-average mean.
8. Stores cleaned measurements, daily summaries and anomaly results in a DuckDB database.
9. Includes unit tests for the transformation logic.

The supplied assessment asks for Python and PySpark, scalability/testability, a brief design and assumptions, and a proof-of-concept rather than an over-engineered application.

## Repository layout

```text
src/
  pipeline.py       Spark transformations and orchestration
  storage.py        DuckDB persistence
tests/
  test_pipeline.py
data/
  data_group_1.csv
  data_group_2.csv
  data_group_3.csv
run_pipeline.py
requirements.txt
README.md
```

## Run

Python 3.11+ and Java 17 are recommended for Spark.

```bash
pip install -r requirements.txt
python run_pipeline.py --input-dir data --output-db output/wind_turbines.duckdb
```

The database contains:

- `cleaned_measurements`
- `turbine_daily_summary`
- `turbine_anomalies`

Run tests:

```bash
pytest -q
```

## Design decisions

### Cleaning

The assessment does not prescribe a specific outlier algorithm. I use two layers:

- domain validation for impossible values (`wind_speed >= 0`, direction 0–359, power output >= 0);
- a per-turbine IQR rule for statistical outliers in the numeric sensor fields, followed by median imputation.

Median imputation is deliberately simple and robust for a small POC. In production, sensor-specific rules and turbine power curves would be preferable.

### Anomaly detection

The assessment defines anomalies as output outside two standard deviations from the mean. I interpret this at the turbine level: calculate each turbine's 24-hour average power, calculate the fleet mean and standard deviation of those turbine averages, and flag turbines outside `mean +/- 2 * stddev`.

This produces a turbine-level anomaly result rather than flagging individual sensor rows.

### Daily files

Although files are updated daily, the supplied sample represents a month and contains repeated 24-hour measurements. The pipeline is idempotent at the measurement key `(timestamp, turbine_id)`, so a rerun does not create duplicate rows in the output database.

## Productionisation discussion

For production I would:

- replace local CSV ingestion with object storage/cloud landing;
- partition raw/cleaned data by event date and turbine group;
- add an incremental watermark/checkpoint;
- use Delta/Iceberg/Hudi or a warehouse/lakehouse instead of local DuckDB;
- add data-quality metrics and alerts;
- use turbine-specific power curves for expected-output anomaly detection;
- add structured logging, configuration, CI, monitoring and lineage;
- quarantine bad records instead of silently dropping them;
- use a proper Spark cluster and tune partitions based on volume.

## Assessment fit

The implementation intentionally keeps the application small and readable. Spark is used for the data processing path; DuckDB is used as a lightweight analytical database for the POC. No AI/LLM dependency or AI-driven logic is used.
