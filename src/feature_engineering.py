from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_transaction_features(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe
        .withColumn("Year", F.year("InvoiceDate"))
        .withColumn("Month", F.month("InvoiceDate"))
        .withColumn("Day", F.dayofmonth("InvoiceDate"))
        .withColumn("Hour", F.hour("InvoiceDate"))
        .withColumn("DayOfWeek", F.date_format("InvoiceDate", "EEEE"))
    )


def customer_aggregates(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("CustomerID").agg(
        F.countDistinct("Invoice").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalQuantity"),
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
        F.countDistinct("StockCode").alias("UniqueProducts"),
        F.round(F.sum("Revenue") / F.countDistinct("Invoice"), 2).alias("AverageOrderValue"),
    )


def product_aggregates(dataframe: DataFrame) -> DataFrame:
    return dataframe.groupBy("StockCode", "Description").agg(
        F.sum("Quantity").alias("TotalQuantitySold"),
        F.round(F.sum("Revenue"), 2).alias("TotalRevenue"),
        F.countDistinct("CustomerID").alias("UniqueCustomers"),
    )
