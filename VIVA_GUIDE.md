# Viva guide

## RFM

`src/segmentation.py` aggregates the cleaned transaction Parquet by customer. Recency is the day difference from the latest transaction in the dataset, frequency is distinct invoice count, and monetary value is cleaned revenue. The four segments are rule-based and deterministic: High-Value, Regular, Occasional, and Inactive.

## Recommendations

`src/recommendations.py` aggregates quantity into implicit customer-product interactions, assigns sorted stable integer IDs, trains Spark MLlib ALS, and removes products already purchased by the customer. The model, mappings, and ranked recommendations are persisted as requested.

## Evaluation and export

`src/evaluation.py` holds out each customer's latest product interaction. It reports RMSE on test interactions that are not cold-start rows, plus Precision@K and Recall@K after removing training purchases. `src/json_export.py` sorts output and limits driver-side collection to 1,000 rows per exported dataset.

## Demo commands

```powershell
python main.py all
python main.py segment
python main.py recommend --top-n 10
python main.py evaluate-recommendations --top-n 10
python main.py export-json
cd frontend
npm install
npm run dev
```
