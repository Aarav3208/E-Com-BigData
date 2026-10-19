import argparse
from pathlib import Path

from src.analytics import run_analytics
from src.config import FRONTEND_DATA_DIR, MODEL_DATA_DIR, OUTPUT_DATA_DIR, PROCESSED_DATA_DIR
from src.data_cleaner import clean_data
from src.data_loader import find_input_file, load_data
from src.evaluation import save_evaluation
from src.feature_engineering import add_transaction_features, customer_aggregates, product_aggregates
from src.json_export import export_json
from src.recommendations import train_recommender
from src.segmentation import save_segments
from src.spark_session import create_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Online Retail II PySpark analytics pipeline")
    parser.add_argument(
        "command",
        choices=[
            "load", "clean", "features", "analytics", "all", "all_records",
            "segment", "recommend", "evaluate-recommendations", "export-json",
        ],
    )
    parser.add_argument("--input", help="CSV path; defaults to data/raw/*.csv")
    parser.add_argument("--limit", type=int, help="Optional number of stored records to display")
    parser.add_argument("--top-n", type=int, default=10, help="Recommendation count (default: 10)")
    args = parser.parse_args()
    spark = create_spark_session()
    try:
        if args.command == "all_records":
            transactions_path = PROCESSED_DATA_DIR / "transactions"
            if not transactions_path.exists():
                raise FileNotFoundError(
                    f"Processed transactions were not found at {transactions_path}. "
                    "Run 'python main.py all' first."
                )
            transactions = spark.read.parquet(str(transactions_path))
            total_records = transactions.count()
            print(f"Stored transaction records: {total_records:,}")
            transactions.printSchema()
            print("Records:")
            for index, record in enumerate(transactions.toLocalIterator(), start=1):
                print(record)
                if args.limit is not None and index >= args.limit:
                    break
            displayed = total_records if args.limit is None else min(args.limit, total_records)
            print(f"Displayed records: {displayed:,}")
            return

        transactions_path = PROCESSED_DATA_DIR / "transactions"

        def read_featured():
            if transactions_path.exists():
                return spark.read.parquet(str(transactions_path))
            raw = clean_data(load_data(spark, args.input))
            featured_data = add_transaction_features(raw)
            featured_data.write.mode("overwrite").parquet(str(transactions_path))
            return featured_data

        if args.command == "segment":
            print("Customer RFM segmentation")
            save_segments(read_featured(), str(PROCESSED_DATA_DIR / "segments"))
            return
        if args.command == "recommend":
            print(f"ALS recommendations (top {args.top_n})")
            train_recommender(
                read_featured(),
                MODEL_DATA_DIR / "als",
                PROCESSED_DATA_DIR / "mappings",
                PROCESSED_DATA_DIR / "recommendations",
                args.top_n,
            )
            return
        if args.command == "evaluate-recommendations":
            print(f"Recommendation evaluation (top {args.top_n})")
            save_evaluation(
                read_featured(),
                PROCESSED_DATA_DIR / "evaluation",
                args.top_n,
            )
            return
        if args.command == "export-json":
            featured = read_featured()
            analytics = run_analytics(featured, OUTPUT_DATA_DIR)
            segments = spark.read.parquet(str(PROCESSED_DATA_DIR / "segments")) if (PROCESSED_DATA_DIR / "segments").exists() else save_segments(featured, str(PROCESSED_DATA_DIR / "segments"))
            recommendations = spark.read.parquet(str(PROCESSED_DATA_DIR / "recommendations")) if (PROCESSED_DATA_DIR / "recommendations").exists() else train_recommender(
                featured, MODEL_DATA_DIR / "als", PROCESSED_DATA_DIR / "mappings", PROCESSED_DATA_DIR / "recommendations", args.top_n
            )[1]
            evaluation = spark.read.parquet(str(PROCESSED_DATA_DIR / "evaluation")) if (PROCESSED_DATA_DIR / "evaluation").exists() else save_evaluation(
                featured, PROCESSED_DATA_DIR / "evaluation", args.top_n
            )
            export_json(
                analytics, segments, product_aggregates(featured),
                recommendations, evaluation, FRONTEND_DATA_DIR,
            )
            print(f"Static JSON written to {FRONTEND_DATA_DIR}")
            return

        raw_file = find_input_file(args.input)
        if args.command == "load":
            print("[1/4] Loading data")
            load_data(spark, raw_file)
        elif args.command == "clean":
            print("[2/4] Cleaning data")
            clean_data(load_data(spark, raw_file)).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "cleaned"))
        elif args.command == "features":
            print("[3/4] Feature engineering")
            cleaned = clean_data(load_data(spark, raw_file))
            featured = add_transaction_features(cleaned)
            featured.write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "transactions"))
            customer_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "customers"))
            product_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "products"))
        elif args.command == "analytics":
            print("[4/4] Spark SQL analytics")
            featured = read_featured()
            run_analytics(featured, OUTPUT_DATA_DIR)
        elif args.command == "all":
            print("[1/4] Loading data")
            loaded = load_data(spark, raw_file)
            print("[2/4] Cleaning data")
            cleaned = clean_data(loaded)
            print("[3/4] Feature engineering")
            featured = add_transaction_features(cleaned)
            featured.write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "transactions"))
            customer_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "customers"))
            product_aggregates(featured).write.mode("overwrite").parquet(str(PROCESSED_DATA_DIR / "products"))
            print("[4/4] Spark SQL analytics")
            run_analytics(featured, OUTPUT_DATA_DIR)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
