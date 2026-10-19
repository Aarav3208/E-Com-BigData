# Big Data Analytics for E-Commerce Customer Behavior

This project implements an end-to-end local PySpark pipeline for the Online Retail II transaction dataset. It covers CSV ingestion, cleaning, feature engineering, Parquet storage, Spark SQL analytics, RFM segmentation, ALS recommendations, evaluation, static JSON export, and a React dashboard.

## Architecture

`CSV dataset -> PySpark cleaning -> transaction/customer/product Parquet -> Spark SQL -> RFM/ALS -> static JSON -> React`


## Technologies

- Python 3.10+ and PySpark 3.5+ (the requirements include NumPy and setuptools for Spark MLlib)
- Apache Spark SQL and Spark MLlib ALS
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

On Windows, Spark uses a project-local filesystem compatibility adapter for local Parquet writes. This is not an HDFS cluster or storage layer; no HDFS commands, namenode, datanode, or HDFS configuration are used.

## Commands

```bash
python main.py load
python main.py clean
python main.py features
python main.py analytics
python main.py all
python main.py all_records
python main.py segment
python main.py recommend --top-n 10
python main.py evaluate-recommendations --top-n 10
python main.py export-json
python -m pytest
```

`features` writes `data/processed/transactions`, `customers`, and `products` as Parquet. `analytics` writes one Spark CSV directory per report under `data/output/`. Spark creates part files inside each output directory.

`all_records` reads the stored transaction Parquet dataset and streams every stored record to the terminal. Use `--limit` for a smaller test display, for example `python main.py all_records --limit 20`. Run `python main.py all` first if the processed transaction dataset does not exist.

`segment` writes RFM values and customer segments to `data/processed/segments`. `recommend` trains an implicit-feedback ALS model, writes it to `data/models/als`, writes stable ID mappings to `data/processed/mappings`, and writes filtered recommendations to `data/processed/recommendations`. `evaluate-recommendations` uses a per-customer temporal holdout and writes RMSE, Precision@K, and Recall@K to `data/processed/evaluation`.

`export-json` creates deterministic, bounded static exports in `frontend/public/data`. It can run after `all`, or it will create the required processed datasets when they are absent. Start the dashboard with `cd frontend; npm install; npm run dev`. The dashboard has no backend: it reads only these JSON files.

You can provide a different CSV explicitly:

```bash
python main.py load --input data/raw/online_retail_II.csv
```

## Completed for the review

- Reusable local SparkSession with a cluster-changeable master setting
- CSV loading, schema/sample display, row counts, duplicate and invalid-record removal
- Cancelled-invoice handling, timestamp conversion, normalized names, and Revenue
- Date/time features and customer/product aggregates
- Parquet processed datasets
- Ten Spark SQL analytics and persisted outputs
- Basic tests and progress messages

## Viva notes

- RFM uses the latest cleaned transaction date as the reference date, distinct invoices for frequency, and cleaned revenue for monetary value.
- ALS uses stable, deterministic integer mappings and implicit purchase strength. Recommendations are anti-joined with every training purchase before ranking.
- Evaluation holds out each customer's latest product interaction. RMSE is reported only for non-cold-start test rows; ranking metrics remove training products.
- JSON collection is bounded to protect the driver, sorted before serialization, and written to the React public directory.
