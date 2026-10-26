from pathlib import Path

from pyspark.sql import DataFrame

from .segmentation import calculate_rfm


def run_analytics(dataframe: DataFrame, output_dir: str | Path) -> dict[str, DataFrame]:
    dataframe.createOrReplaceTempView("transactions")
    queries = {
        "total_revenue": "SELECT ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions",
        "total_orders": "SELECT COUNT(DISTINCT Invoice) AS TotalOrders FROM transactions",
        "total_customers": "SELECT COUNT(DISTINCT CustomerID) AS TotalCustomers FROM transactions",
        "total_products": "SELECT COUNT(DISTINCT StockCode) AS TotalProducts FROM transactions",
        "revenue_by_country": "SELECT Country, ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions GROUP BY Country ORDER BY TotalRevenue DESC",
        "top_products_by_revenue": "SELECT StockCode, first(Description) AS Description, ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions GROUP BY StockCode ORDER BY TotalRevenue DESC LIMIT 10",
        "top_products_by_quantity": "SELECT StockCode, first(Description) AS Description, SUM(Quantity) AS TotalQuantitySold FROM transactions GROUP BY StockCode ORDER BY TotalQuantitySold DESC LIMIT 10",
        "monthly_revenue": "SELECT Year, Month, ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions GROUP BY Year, Month ORDER BY Year, Month",
        "monthly_orders": "SELECT Year, Month, COUNT(DISTINCT Invoice) AS TotalOrders FROM transactions GROUP BY Year, Month ORDER BY Year, Month",
        "top_customers_by_revenue": "SELECT CustomerID, ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions GROUP BY CustomerID ORDER BY TotalRevenue DESC LIMIT 10",
        "average_order_value": "SELECT ROUND(SUM(Revenue) / COUNT(DISTINCT Invoice), 2) AS AverageOrderValue FROM transactions",
        "customer_behavior_summary": """
            SELECT
                COUNT(*) AS TotalCustomers,
                SUM(CASE WHEN TotalOrders = 1 THEN 1 ELSE 0 END) AS OneTimeCustomers,
                SUM(CASE WHEN TotalOrders > 1 THEN 1 ELSE 0 END) AS RepeatCustomers,
                ROUND(SUM(CASE WHEN TotalOrders > 1 THEN 1 ELSE 0 END) / COUNT(*), 4) AS RepeatPurchaseRate,
                ROUND(AVG(TotalOrders), 2) AS AverageOrdersPerCustomer,
                ROUND(AVG(TotalRevenue), 2) AS AverageCustomerSpending
            FROM (
                SELECT CustomerID, COUNT(DISTINCT Invoice) AS TotalOrders, SUM(Revenue) AS TotalRevenue
                FROM transactions
                GROUP BY CustomerID
            )
        """,
        "purchase_frequency_distribution": """
            SELECT TotalOrders, COUNT(*) AS CustomerCount
            FROM (
                SELECT CustomerID, COUNT(DISTINCT Invoice) AS TotalOrders
                FROM transactions
                GROUP BY CustomerID
            )
            GROUP BY TotalOrders
            ORDER BY TotalOrders
        """,
        "segment_summary": """
            SELECT Segment, COUNT(*) AS CustomerCount,
                ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS CustomerPercentage,
                ROUND(SUM(Monetary), 2) AS SegmentRevenue
            FROM customer_rfm
            GROUP BY Segment
            ORDER BY CustomerCount DESC, Segment
        """,
    }
    results = {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    calculate_rfm(dataframe).createOrReplaceTempView("customer_rfm")
    for name, query in queries.items():
        result = dataframe.sparkSession.sql(query)
        results[name] = result
        print(f"\n{name}")
        result.show(10, truncate=False)
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path / name))
    return results
