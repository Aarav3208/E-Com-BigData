import argparse
from pathlib import Path

from src.analytics import run_analytics
from src.config import OUTPUT_DATA_DIR, PROCESSED_DATA_DIR
from src.data_cleaner import clean_data
from src.data_loader import find_input_file, load_data
from src.feature_engineering import add_transaction_features, customer_aggregates, product_aggregates
from src.hdfs_manager import setup_hdfs
from src.spark_session import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Online Retail II PySpark analytics pipeline")
    parser.add_argument("command", choices=["load", "clean", "features", "hdfs", "analytics", "all"])
    parser.add_argument("--input", help="CSV path; defaults to data/raw/*.csv")
    args = parser.parse_args()
    spark = create_spark_session()
    try:
        raw_file = find_input_file(args.input)
        if args.command == "load":
            print("[1/5] Loading data")
            load_data(spark, raw_file)
        elif args.command == "clean":
            print("[2/5] Cleaning data")
            clean_data(load_data(spark, raw_file)).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "cleaned"))
        elif args.command == "features":
            print("[3/5] Feature engineering")
            cleaned = clean_data(load_data(spark, raw_file))
            featured = add_transaction_features(cleaned)
            featured.write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "transactions"))
            customer_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "customers"))
            product_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "products"))
        elif args.command == "hdfs":
            print("[4/5] HDFS storage")
            setup_hdfs(raw_file)
        elif args.command == "analytics":
            print("[5/5] Spark SQL analytics")
            featured = spark.read.parquet(str(PROCESSED_DATA_DIR / "transactions"))
            run_analytics(featured, OUTPUT_DATA_DIR)
        else:
            print("[1/5] Loading data")
            loaded = load_data(spark, raw_file)
            print("[2/5] Cleaning data")
            cleaned = clean_data(loaded)
            print("[3/5] Feature engineering")
            featured = add_transaction_features(cleaned)
            featured.write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "transactions"))
            customer_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "customers"))
            product_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "products"))
            print("[4/5] HDFS storage")
            setup_hdfs(raw_file)
            print("[5/5] Spark SQL analytics")
            run_analytics(featured, OUTPUT_DATA_DIR)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
