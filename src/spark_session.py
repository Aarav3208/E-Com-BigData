import os

from pyspark.sql import SparkSession

from .config import LOCAL_SPARK_DIR, LOCAL_SPARK_FS_JAR, SPARK_APP_NAME, SPARK_MASTER

LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"


def _configure_windows_local_filesystem(builder):
    """Use a local-only adapter required for Spark Parquet writes on Windows."""
    if os.name != "nt" or not LOCAL_SPARK_FS_JAR.is_file():
        return builder
    os.environ.setdefault("HADOOP_HOME", str(LOCAL_SPARK_DIR))
    return (
        builder
        .config("spark.hadoop.hadoop.home.dir", str(LOCAL_SPARK_DIR))
        .config("spark.hadoop.fs.file.impl", LOCAL_FS_CLASS)
        .config("spark.driver.extraClassPath", str(LOCAL_SPARK_FS_JAR))
        .config("spark.executor.extraClassPath", str(LOCAL_SPARK_FS_JAR))
    )


def create_spark_session(master: str = SPARK_MASTER) -> SparkSession:
    """Create the reusable Spark entry point; change master for a cluster later."""
    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(master)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    return _configure_windows_local_filesystem(builder).getOrCreate()
