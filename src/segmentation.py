from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_rfm(dataframe: DataFrame) -> DataFrame:
    """Calculate customer RFM values from already-cleaned transactions."""
    reference_date = dataframe.agg(F.max("InvoiceDate").alias("max_date")).first()["max_date"]
    if reference_date is None:
        return dataframe.sparkSession.createDataFrame(
            [], "CustomerID string, Recency int, Frequency long, Monetary double, Segment string"
        )

    return (
        dataframe.groupBy("CustomerID")
        .agg(
            F.datediff(F.lit(reference_date), F.max("InvoiceDate")).cast("int").alias("Recency"),
            F.countDistinct("Invoice").alias("Frequency"),
            F.round(F.sum("Revenue"), 2).alias("Monetary"),
        )
        .withColumn(
            "Segment",
            F.when(
                (F.col("Recency") <= 90)
                & (F.col("Frequency") >= 5)
                & (F.col("Monetary") >= 500),
                "High-Value",
            )
            .when(
                (F.col("Recency") <= 180)
                & (F.col("Frequency") >= 2)
                & (F.col("Monetary") >= 100),
                "Regular",
            )
            .when(F.col("Recency") <= 365, "Occasional")
            .otherwise("Inactive"),
        )
    )


def save_segments(dataframe: DataFrame, output_path: str) -> DataFrame:
    segments = calculate_rfm(dataframe)
    segments.write.mode("overwrite").parquet(output_path)
    return segments
