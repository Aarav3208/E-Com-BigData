import os

from pyspark.sql import SparkSession

from .config import (
    BARE_LOCAL_FS_JAR,
    LOCAL_HADOOP_DIR,
    SPARK_APP_NAME,
    SPARK_MASTER,
)

BARE_LOCAL_FS_CLASS = "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"


def _configure_windows_local_filesystem(builder):
    """Avoid winutils for local Spark files without starting an HDFS cluster."""
    if os.name != "nt" or not BARE_LOCAL_FS_JAR.is_file():
        return builder

    os.environ.setdefault("HADOOP_HOME", str(LOCAL_HADOOP_DIR))
    return (
        builder
        .config("spark.hadoop.hadoop.home.dir", str(LOCAL_HADOOP_DIR))
        .config("spark.hadoop.fs.file.impl", BARE_LOCAL_FS_CLASS)
        .config("spark.driver.extraClassPath", str(BARE_LOCAL_FS_JAR))
        .config("spark.executor.extraClassPath", str(BARE_LOCAL_FS_JAR))
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
