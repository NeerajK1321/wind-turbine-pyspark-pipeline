from src.pipeline import run_pipeline

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data")
    parser.add_argument("--output-db", default="output/wind_turbines.duckdb")
    args = parser.parse_args()

    run_pipeline(args.input_dir, args.output_db)
