from pathlib import Path

import setuptools  # noqa: F401  # PySpark 3.5 imports distutils on Python 3.14.
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_interactions(dataframe: DataFrame) -> DataFrame:
    """Create one positive interaction per customer/product pair."""
    return dataframe.groupBy("CustomerID", "StockCode").agg(
        F.sum("Quantity").cast("float").alias("rating")
    )


def add_stable_ids(interactions: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    customer_window = Window.orderBy("CustomerID")
    product_window = Window.orderBy("StockCode")
    customers = (
        interactions.select("CustomerID").distinct()
        .withColumn("user_id", (F.row_number().over(customer_window) - 1).cast("int"))
    )
    products = (
        interactions.select("StockCode").distinct()
        .withColumn("item_id", (F.row_number().over(product_window) - 1).cast("int"))
    )
    numeric = (
        interactions.join(customers, "CustomerID")
        .join(products, "StockCode")
        .select("CustomerID", "StockCode", "user_id", "item_id", "rating")
    )
    return numeric, customers, products


def train_recommender(
    dataframe: DataFrame,
    model_path: str | Path,
    mappings_path: str | Path,
    recommendations_path: str | Path,
    top_n: int = 10,
) -> tuple[ALSModel, DataFrame]:
    if top_n < 1:
        raise ValueError("top_n must be at least 1")
    interactions = build_interactions(dataframe)
    numeric, customers, products = add_stable_ids(interactions)
    model = ALS(
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        rank=10,
        maxIter=10,
        regParam=0.1,
        nonnegative=True,
        implicitPrefs=True,
        coldStartStrategy="drop",
        seed=42,
    ).fit(numeric.select("user_id", "item_id", "rating"))

    descriptions = dataframe.groupBy("StockCode").agg(
        F.first("Description", ignorenulls=True).alias("Description")
    )
    recommendations = (
        model.recommendForAllUsers(top_n)
        .select("user_id", F.explode("recommendations").alias("recommendation"))
        .select(
            "user_id",
            F.col("recommendation.item_id").alias("item_id"),
            F.col("recommendation.rating").alias("score"),
        )
        .join(customers, "user_id")
        .join(products, "item_id")
        .join(
            numeric.select("user_id", "item_id").withColumn("purchased", F.lit(True)),
            ["user_id", "item_id"],
            "left",
        )
        .filter(F.col("purchased").isNull())
        .drop("purchased")
        .join(descriptions, "StockCode", "left")
        .select("CustomerID", "StockCode", "Description", "score")
        .withColumn(
            "rank",
            F.row_number().over(Window.partitionBy("CustomerID").orderBy(F.desc("score"), F.asc("StockCode"))),
        )
        .filter(F.col("rank") <= top_n)
    )
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    Path(mappings_path).mkdir(parents=True, exist_ok=True)
    Path(recommendations_path).parent.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(model_path))
    customers.write.mode("overwrite").parquet(str(Path(mappings_path) / "customers"))
    products.write.mode("overwrite").parquet(str(Path(mappings_path) / "products"))
    recommendations.write.mode("overwrite").parquet(str(recommendations_path))
    return model, recommendations


def load_recommender(model_path: str | Path) -> ALSModel:
    return ALSModel.load(str(model_path))
