# Big Data Analytics for E-Commerce Customer Behavior

This project implements the first review milestone for the Online Retail II transaction dataset. It demonstrates a beginner-friendly pipeline from CSV ingestion through cleaning, feature engineering, Parquet storage, optional HDFS storage, and Spark SQL analytics.

## Architecture

`CSV dataset -> PySpark cleaning -> transaction/customer/product Parquet -> Spark SQL -> CSV analytics`

HDFS is supported as an optional distributed storage stage. The current implementation intentionally does not include recommendations, MongoDB, dashboards, APIs, or deployment services.

## Technologies

- Python 3.10+ and PySpark 3.5+
- Apache Hadoop HDFS (optional for local demonstration)
- Apache Spark SQL
- CSV input and Parquet processed data
- Pytest for three focused checks

## Dataset placement

Place the downloaded file at `data/raw/online_retail_II.csv`. The current workspace also supports the existing root-level `online_retail_II.csv` as a temporary fallback. The expected columns are `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, and `Country`.

## Installation

In PowerShell or Git Bash from this directory:

```bash
python -m venv .venv
# PowerShell
.venv\\Scripts\\Activate.ps1
# Git Bash
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Java 8 or newer is required by Spark. On Windows, set `JAVA_HOME` to the JDK directory before running Spark. Do not download the dataset automatically; place it manually as described above.

### Windows local Spark setup

PySpark 3.5.9 bundles Hadoop 3.3.4. This project uses the small, pure-Java GlobalMentor local filesystem adapter in `hadoop-local/lib` so local Spark can write Parquet without `winutils.exe` and without installing or starting an HDFS cluster. The adapter is loaded automatically by `src/spark_session.py` when that project-local JAR exists. Keep `hadoop-local/` outside Git; it is ignored in `.gitignore`.

Download the pinned adapter once if `hadoop-local/lib/hadoop-bare-naked-local-fs-0.1.0.jar` is missing:

```powershell
New-Item -ItemType Directory -Force hadoop-local/lib
Invoke-WebRequest `
	-Uri https://repo1.maven.org/maven2/com/globalmentor/hadoop-bare-naked-local-fs/0.1.0/hadoop-bare-naked-local-fs-0.1.0.jar `
	-OutFile hadoop-local/lib/hadoop-bare-naked-local-fs-0.1.0.jar
```

The application sets project-local `HADOOP_HOME` and `hadoop.home.dir`; it does not modify system-wide Hadoop settings. The `hdfs` command remains optional and is separate from local Parquet processing.

## Commands

```bash
python main.py load
python main.py clean
python main.py features
python main.py hdfs
python main.py analytics
python main.py all
python -m pytest
```

`features` writes `data/processed/transactions`, `customers`, and `products` as Parquet. `analytics` writes one Spark CSV directory per report under `data/output/`. Spark creates part files inside each output directory.

You can provide a different CSV explicitly:

```bash
python main.py load --input data/raw/online_retail_II.csv
```

## Hadoop/HDFS setup

Install Hadoop, configure the local NameNode and DataNode, format the NameNode once, and start both services. Add Hadoop's `bin` directory to `PATH`, then set `HDFS_NAMENODE` in `src/config.py` if your NameNode uses another URI or port. Run:

```bash
python main.py hdfs
```

This creates `/ecommerce/raw`, `/ecommerce/processed`, and `/ecommerce/analytics`, uploads the raw CSV, and verifies the tree with `hdfs dfs -ls`. If HDFS is not installed or running, the command prints setup guidance and the rest of the local pipeline remains usable.

## Completed for the review

- Reusable local SparkSession with a cluster-changeable master setting
- CSV loading, schema/sample display, row counts, duplicate and invalid-record removal
- Cancelled-invoice handling, timestamp conversion, normalized names, and Revenue
- Date/time features and customer/product aggregates
- Parquet processed datasets
- Optional HDFS utility
- Ten Spark SQL analytics and persisted outputs
- Basic tests and progress messages

## Future work

1. Collaborative filtering recommendation engine using Spark MLlib ALS
2. Recommendation evaluation
3. MongoDB recommendation storage
4. Customer segmentation/RFM
5. Power BI dashboard
6. Final end-to-end deployment
