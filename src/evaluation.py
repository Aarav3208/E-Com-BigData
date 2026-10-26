from pathlib import Path

import setuptools  # noqa: F401  # PySpark 3.5 imports distutils on Python 3.14.
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .recommendations import add_stable_ids


def temporal_train_test(interactions: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Hold out each customer's latest product without sharing pair data."""
    latest = Window.partitionBy("CustomerID").orderBy(
        F.desc("last_purchase"), F.asc("StockCode")
    )
    ranked = interactions.select(
        "CustomerID",
        "StockCode",
        "rating",
        F.col("InvoiceDate").alias("last_purchase"),
    ).withColumn("holdout_rank", F.row_number().over(latest))
    return (
        ranked.filter(F.col("holdout_rank") > 1).drop("holdout_rank"),
        ranked.filter(F.col("holdout_rank") == 1).drop("holdout_rank"),
    )


def _ranking_metrics(
    ranked: DataFrame, test: DataFrame, top_k: int
) -> dict[str, float]:
    eligible = test.select("CustomerID").distinct()
    hits = (
        ranked.filter(F.col("rank") <= top_k)
        .join(test.select("CustomerID", "StockCode"), ["CustomerID", "StockCode"], "left_semi")
        .select("CustomerID", "rank")
    )
    hit_count = hits.count()
    user_count = eligible.count()
    denominator = user_count * top_k
    return {
        "Precision@K": hit_count / denominator if denominator else 0.0,
        "Recall@K": hit_count / user_count if user_count else 0.0,
        "MAP@K": (
            hits.withColumn("average_precision", F.lit(1.0) / F.col("rank"))
            .agg(F.sum("average_precision"))
            .first()[0] / user_count
            if user_count else 0.0
        ),
        "NDCG@K": (
            hits.withColumn(
                "discounted_gain",
                F.lit(1.0) / (F.log2(F.col("rank") + F.lit(1.0))),
            ).agg(F.sum("discounted_gain")).first()[0] / user_count
            if user_count else 0.0
        ),
    }


def _rank_unseen(
    candidates: DataFrame, train: DataFrame, top_k: int
) -> DataFrame:
    return (
        candidates.join(
            train.select("CustomerID", "StockCode").distinct(),
            ["CustomerID", "StockCode"],
            "leftanti",
        )
        .withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("CustomerID").orderBy(
                    F.desc("score"), F.asc("StockCode")
                )
            ),
        )
        .filter(F.col("rank") <= top_k)
    )


def evaluate_recommendations(dataframe: DataFrame, top_k: int = 10) -> DataFrame:
    if top_k < 1:
        raise ValueError("top_k must be at least 1")
    train, test = temporal_train_test(build_interactions_with_dates(dataframe))
    if not train.take(1):
        return _metrics_frame(dataframe, {"ALS": {}, "Popularity baseline": {}})

    numeric, customers, products = add_stable_ids(train)
    als = ALS(
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        rank=10,
        maxIter=10,
        regParam=0.1,
        implicitPrefs=True,
        coldStartStrategy="drop",
        seed=42,
    ).fit(numeric.select("user_id", "item_id", "rating"))

    test_numeric = (
        test.join(customers, "CustomerID")
        .join(products, "StockCode")
        .select("user_id", "item_id", "rating")
    )
    predictions = als.transform(test_numeric).dropna(subset=["prediction"])
    rmse = (
        RegressionEvaluator(
            metricName="rmse", labelCol="rating", predictionCol="prediction"
        ).evaluate(predictions)
        if predictions.take(1)
        else None
    )

    purchase_counts = numeric.groupBy("user_id").count().agg(F.max("count")).first()[0] or 0
    candidate_count = min(products.select("item_id").count(), top_k + purchase_counts)
    als_candidates = (
        als.recommendForAllUsers(candidate_count)
        .select("user_id", F.posexplode("recommendations").alias("position", "recommendation"))
        .select(
            "user_id",
            F.col("recommendation.item_id").alias("item_id"),
            F.col("recommendation.rating").alias("score"),
        )
        .join(customers, "user_id")
        .join(products, "item_id")
        .select("CustomerID", "StockCode", "score")
    )
    als_ranked = _rank_unseen(als_candidates, train, top_k)
    als_metrics = _ranking_metrics(als_ranked, test, top_k)
    als_metrics["RMSE"] = rmse

    popular = (
        train.groupBy("StockCode")
        .agg(F.sum("rating").alias("score"))
        .orderBy(F.desc("score"), F.asc("StockCode"))
        .limit(candidate_count)
    )
    baseline_candidates = (
        test.select("CustomerID").distinct()
        .crossJoin(popular)
    )
    baseline_ranked = _rank_unseen(baseline_candidates, train, top_k)
    baseline_metrics = _ranking_metrics(baseline_ranked, test, top_k)
    return _metrics_frame(
        dataframe,
        {"ALS": als_metrics, "Popularity baseline": baseline_metrics},
    )


def _metrics_frame(
    dataframe: DataFrame, metrics_by_model: dict[str, dict[str, float | None]]
) -> DataFrame:
    spark = dataframe.sparkSession
    result = None
    for model, metrics in metrics_by_model.items():
        for metric, value in metrics.items():
            row = spark.range(1).select(
                F.lit(model).alias("model"),
                F.lit(metric).alias("metric"),
                F.lit(value if value is not None else float("nan"))
                .cast("double")
                .alias("value"),
            )
            result = row if result is None else result.union(row)
    if result is None:
        return spark.range(0).select(
            F.lit(None).cast("string").alias("model"),
            F.lit(None).cast("string").alias("metric"),
            F.lit(None).cast("double").alias("value"),
        )
    return result


def build_interactions_with_dates(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("CustomerID", "StockCode").agg(
        F.sum("Quantity").cast("float").alias("rating"),
        F.max("InvoiceDate").alias("InvoiceDate"),
    )


def save_evaluation(
    dataframe: DataFrame, output_path: str | Path, top_k: int = 10
) -> DataFrame:
    result = evaluate_recommendations(dataframe, top_k)
    result.write.mode("overwrite").parquet(str(output_path))
    return result
