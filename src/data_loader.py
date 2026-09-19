from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config import RAW_DATA_DIR, PROJECT_ROOT


def find_input_file(input_path: str | Path | None = None) -> Path:
    """Find a CSV in data/raw, with a root-level fallback for this workspace."""
    if input_path:
        path = Path(input_path)
        if path.is_file():
            return path
        raise FileNotFoundError(f"Input file does not exist: {path}")

    candidates = sorted(RAW_DATA_DIR.glob("*.csv"))
    if not candidates:
        candidates = sorted(PROJECT_ROOT.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No CSV found. Place the dataset under {RAW_DATA_DIR} and try again."
        )
    return candidates[0]


def load_data(spark: SparkSession, input_path: str | Path | None = None) -> DataFrame:
    path = find_input_file(input_path)
    dataframe = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(path))
    )
    print(f"Loaded: {path}")
    print(f"Row count: {dataframe.count():,}")
    dataframe.printSchema()
    dataframe.show(5, truncate=False)
    return dataframe
