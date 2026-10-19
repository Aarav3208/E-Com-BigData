from pathlib import Path

import setuptools  # noqa: F401  # PySpark 3.5 imports distutils on Python 3.14.
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .recommendations import add_stable_ids


def temporal_train_test(interactions: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Hold out each customer's latest product without sharing pair data."""
    latest = Window.partitionBy("CustomerID").orderBy(F.desc("last_purchase"), F.asc("StockCode"))
    with_dates = interactions.select(
        "CustomerID", "StockCode", "rating",
        F.col("InvoiceDate").alias("last_purchase"),
    )
    ranked = with_dates.withColumn("holdout_rank", F.row_number().over(latest))
    test = ranked.filter(F.col("holdout_rank") == 1).drop("holdout_rank")
    train = ranked.filter(F.col("holdout_rank") > 1).drop("holdout_rank")
    # Customers with one product need a test row but cannot train an ALS user.
    return train, test


def evaluate_recommendations(dataframe: DataFrame, top_k: int = 10) -> DataFrame:
    if top_k < 1:
        raise ValueError("top_k must be at least 1")
    train, test = temporal_train_test(build_interactions_with_dates(dataframe))
    if not train.take(1):
        return _metrics_frame(dataframe, None, 0.0, 0.0)
    numeric, customers, products = add_stable_ids(train)
    als = ALS(
        userCol="user_id", itemCol="item_id", ratingCol="rating", rank=10,
        maxIter=10, regParam=0.1, implicitPrefs=True, coldStartStrategy="drop", seed=42,
    ).fit(numeric.select("user_id", "item_id", "rating"))
    test_numeric = (
        test.join(customers, "CustomerID")
        .join(products, "StockCode")
        .select("user_id", "item_id", "rating")
    )
    predictions = als.transform(test_numeric).dropna(subset=["prediction"])
    rmse = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    ).evaluate(predictions) if predictions.take(1) else None

    test_users = test.select("CustomerID").distinct()
    # Use the same stable mappings and remove every training purchase before ranking.
    train_numeric = numeric
    candidates = (
        als.recommendForAllUsers(top_k + 20)
        .select("user_id", F.explode("recommendations").alias("r"))
        .select("user_id", F.col("r.item_id").alias("item_id"))
        .join(
            train_numeric.select("user_id", "item_id").distinct(),
            ["user_id", "item_id"], "leftanti",
        )
        .join(customers, "user_id")
        .join(products, "item_id")
        .join(test.select("CustomerID", "StockCode"), ["CustomerID", "StockCode"], "left_semi")
        .select("CustomerID", "StockCode")
    )
    hits = candidates.count()
    relevant = test.join(test_users, "CustomerID").count()
    recommended = (
        als.recommendForAllUsers(top_k)
        .select(F.explode("recommendations").alias("r"))
        .count()
    )
    precision = hits / recommended if recommended else 0.0
    recall = hits / relevant if relevant else 0.0
    return _metrics_frame(dataframe, rmse, precision, recall)


def _metrics_frame(
    dataframe: DataFrame, rmse: float | None, precision: float, recall: float
) -> DataFrame:
    metrics = [
        ("RMSE", rmse if rmse is not None else float("nan")),
        ("Precision@K", precision),
        ("Recall@K", recall),
    ]
    result = dataframe.sparkSession.range(1).select(
        F.lit(metrics[0][0]).alias("metric"), F.lit(metrics[0][1]).cast("double").alias("value")
    )
    for name, value in metrics[1:]:
        result = result.union(
            dataframe.sparkSession.range(1).select(
                F.lit(name).alias("metric"), F.lit(value).cast("double").alias("value")
            )
        )
    return result


def build_interactions_with_dates(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("CustomerID", "StockCode").agg(
        F.sum("Quantity").cast("float").alias("rating"),
        F.max("InvoiceDate").alias("InvoiceDate"),
    )


def save_evaluation(dataframe: DataFrame, output_path: str | Path, top_k: int = 10) -> DataFrame:
    result = evaluate_recommendations(dataframe, top_k)
    result.write.mode("overwrite").parquet(str(output_path))
    return result
