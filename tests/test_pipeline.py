from src.data_cleaner import clean_data
from src.feature_engineering import customer_aggregates


def test_cleaning_removes_invalid_records(spark_session):
    dataframe = spark_session.sql("""
        SELECT * FROM VALUES
            ('1', 'A', 'Valid', 2, '2010-01-01 10:00:00', 5.0, '100', 'UK'),
            ('C2', 'A', 'Cancelled', 2, '2010-01-01 10:00:00', 5.0, '100', 'UK'),
            ('3', 'A', 'Bad quantity', 0, '2010-01-01 10:00:00', 5.0, '100', 'UK'),
            ('4', 'A', 'Missing customer', 1, '2010-01-01 10:00:00', 5.0, NULL, 'UK')
        AS records(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, `Customer ID`, Country)
    """)
    cleaned = clean_data(dataframe)
    assert cleaned.count() == 1


def test_revenue_is_quantity_times_price(spark_session):
    dataframe = spark_session.sql("""
        SELECT * FROM VALUES
            ('1', 'A', 'Item', 3, '2010-01-01 10:00:00', 4.5, '100', 'UK')
        AS records(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, `Customer ID`, Country)
    """)
    result = clean_data(dataframe).first()
    assert result.Revenue == 13.5


def test_customer_aggregation(spark_session):
    dataframe = spark_session.sql("""
        SELECT * FROM VALUES
            ('1', 'A', 'Item A', 2, '2010-01-01 10:00:00', 5.0, '100', 'UK'),
            ('2', 'B', 'Item B', 1, '2010-01-02 10:00:00', 10.0, '100', 'UK')
        AS records(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, `Customer ID`, Country)
    """)
    result = customer_aggregates(clean_data(dataframe)).first()
    assert result.TotalOrders == 2
    assert result.TotalQuantity == 3
    assert result.TotalRevenue == 20.0
    assert result.AverageOrderValue == 10.0
