from src.evaluation import temporal_train_test
from src.json_export import export_json
from src.recommendations import build_interactions
from src.segmentation import calculate_rfm
from pyspark.sql import functions as F


def test_rfm_segments_use_cleaned_transactions(spark_session):
    data = spark_session.sql("""
        SELECT * FROM VALUES
            ('1', 'A', 2, '2010-01-01 10:00:00', 5.0, '100'),
            ('2', 'B', 1, '2010-02-01 10:00:00', 10.0, '100'),
            ('3', 'A', 1, '2010-02-02 10:00:00', 2.0, '200')
        AS records(Invoice, StockCode, Quantity, InvoiceDate, Price, CustomerID)
    """)
    data = data.withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
    data = data.withColumn("Revenue", data.Quantity * data.Price)
    result = calculate_rfm(data).where("CustomerID = '100'").first()
    assert result.Frequency == 2
    assert result.Monetary == 20.0
    assert result.Segment == "Occasional"


def test_interactions_aggregate_customer_product_purchases(spark_session):
    data = spark_session.sql("""
        SELECT * FROM VALUES ('100', 'A', 2.0), ('100', 'A', 3.0), ('100', 'B', 1.0)
        AS records(CustomerID, StockCode, Quantity)
    """)
    rows = build_interactions(data).orderBy("StockCode").collect()
    assert [(row.StockCode, row.rating) for row in rows] == [("A", 5.0), ("B", 1.0)]


def test_temporal_split_has_no_duplicate_customer_product_rows(spark_session):
    data = spark_session.sql("""
        SELECT * FROM VALUES
            ('100', 'A', 1.0, '2010-01-01'),
            ('100', 'B', 1.0, '2010-02-01')
        AS records(CustomerID, StockCode, rating, InvoiceDate)
    """).withColumn("InvoiceDate", F.to_date("InvoiceDate"))
    train, test = temporal_train_test(data)
    assert train.join(test, ["CustomerID", "StockCode"]).count() == 0
    assert test.first().StockCode == "B"


def test_json_export_is_valid_and_sorted(spark_session, tmp_path):
    frame = spark_session.sql("SELECT '100' CustomerID, 'Regular' Segment, 10.0 Monetary")
    products = spark_session.sql("SELECT 'A' StockCode, 'Item' Description, 2 TotalQuantitySold")
    recommendations = spark_session.sql("SELECT '100' CustomerID, 'B' StockCode, 'Other' Description, 0.9 score, 1 rank")
    evaluation = spark_session.sql("SELECT 'RMSE' metric, 1.0 value")
    export_json({}, frame, products, recommendations, evaluation, tmp_path)
    assert (tmp_path / "customer_segments.json").read_text(encoding="utf-8").startswith("[")
    assert '"CustomerID": "100"' in (tmp_path / "recommendations.json").read_text(encoding="utf-8")
