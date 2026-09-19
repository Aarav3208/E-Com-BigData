from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DATA_DIR = PROJECT_ROOT / "data" / "output"
LOCAL_HADOOP_DIR = PROJECT_ROOT / "hadoop-local"
BARE_LOCAL_FS_JAR = LOCAL_HADOOP_DIR / "lib" / "hadoop-bare-naked-local-fs-0.1.0.jar"

SPARK_APP_NAME = "EcommerceCustomerBehavior"
SPARK_MASTER = "local[*]"
HDFS_NAMENODE = "hdfs://localhost:9000"
HDFS_BASE_PATH = "/ecommerce"
