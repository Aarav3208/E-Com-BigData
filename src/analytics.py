from pathlib import Path

from pyspark.sql import DataFrame


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
        "top_customers_by_revenue": "SELECT CustomerID, ROUND(SUM(Revenue), 2) AS TotalRevenue FROM transactions GROUP BY CustomerID ORDER BY TotalRevenue DESC LIMIT 10",
        "average_order_value": "SELECT ROUND(SUM(Revenue) / COUNT(DISTINCT Invoice), 2) AS AverageOrderValue FROM transactions",
    }
    results = {}
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    for name, query in queries.items():
        result = dataframe.sparkSession.sql(query)
        results[name] = result
        print(f"\n{name}")
        result.show(10, truncate=False)
        result.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path / name))
    return results
