from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DATA_DIR = PROJECT_ROOT / "data" / "output"
MODEL_DATA_DIR = PROJECT_ROOT / "data" / "models"
FRONTEND_DATA_DIR = PROJECT_ROOT / "frontend" / "public" / "data"
LOCAL_SPARK_DIR = PROJECT_ROOT / "hadoop-local"
LOCAL_SPARK_FS_JAR = LOCAL_SPARK_DIR / "lib" / "hadoop-bare-naked-local-fs-0.1.0.jar"
SPARK_APP_NAME = "EcommerceCustomerBehavior"
SPARK_MASTER = "local[*]"
