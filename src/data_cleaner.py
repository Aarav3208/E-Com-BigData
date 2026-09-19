from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_columns(dataframe: DataFrame) -> DataFrame:
    """Use stable names throughout the pipeline."""
    names = {
        "Invoice": "Invoice",
        "StockCode": "StockCode",
        "Description": "Description",
        "Quantity": "Quantity",
        "InvoiceDate": "InvoiceDate",
        "Price": "Price",
        "Customer ID": "CustomerID",
        "Country": "Country",
    }
    for old_name, new_name in names.items():
        if old_name in dataframe.columns:
            dataframe = dataframe.withColumnRenamed(old_name, new_name)
    return dataframe


def clean_data(dataframe: DataFrame) -> DataFrame:
    before = dataframe.count()
    cleaned = normalize_columns(dataframe)
    cleaned = (
        cleaned
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
        .withColumn("CustomerID", F.col("CustomerID").cast("string"))
        .withColumn("Quantity", F.col("Quantity").cast("integer"))
        .withColumn("Price", F.col("Price").cast("double"))
        .withColumn("Invoice", F.col("Invoice").cast("string"))
        .withColumn("StockCode", F.trim(F.col("StockCode")))
        .withColumn("Revenue", F.col("Quantity") * F.col("Price"))
        .filter(F.col("CustomerID").isNotNull())
        .filter(F.col("StockCode").isNotNull() & (F.col("StockCode") != ""))
        .filter(F.col("Quantity") > 0)
        .filter(F.col("Price") > 0)
        .filter(F.col("InvoiceDate").isNotNull())
        .filter(~F.upper(F.col("Invoice")).startswith("C"))
        .dropDuplicates()
    )
    after = cleaned.count()
    print(f"Cleaning rows: {before:,} -> {after:,}")
    return cleaned
