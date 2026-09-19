import pytest

from src.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark_session():
    session = create_spark_session("local[2]")
    yield session
    session.stop()
