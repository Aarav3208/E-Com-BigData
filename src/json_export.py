import json
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame

from .config import FRONTEND_DATA_DIR


MAX_ROWS = 1000
ENTITY_MAX_ROWS = 50000
RECOMMENDATION_MAX_ROWS = 50000


def _records(dataframe: DataFrame, limit: int = MAX_ROWS) -> list[dict[str, Any]]:
    rows = dataframe.limit(limit).collect()
    return [row.asDict(recursive=True) for row in rows]


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def export_json(
    analytics: dict[str, DataFrame],
    segments: DataFrame,
    products: DataFrame,
    recommendations: DataFrame,
    evaluation: DataFrame,
    output_dir: str | Path = FRONTEND_DATA_DIR,
) -> Path:
    output_path = Path(output_dir)
    evaluation_order = (
        ["model", "metric"] if "model" in evaluation.columns else ["metric"]
    )
    analytics_summary = {
        name: _records(frame.orderBy(*frame.columns), 100)
        for name, frame in sorted(analytics.items())
    }
    _write_json(output_path / "analytics.json", analytics_summary)
    _write_json(output_path / "customer_segments.json", _records(
        segments.orderBy("CustomerID"), ENTITY_MAX_ROWS
    ))
    _write_json(output_path / "products.json", _records(
        products.orderBy("StockCode"), ENTITY_MAX_ROWS
    ))
    _write_json(output_path / "recommendations.json", _records(
        recommendations.orderBy("CustomerID", "rank", "StockCode"),
        RECOMMENDATION_MAX_ROWS,
    ))
    _write_json(output_path / "evaluation.json", _records(
        evaluation.orderBy(*evaluation_order), ENTITY_MAX_ROWS
    ))
    return output_path


def copy_export_to_frontend(source_dir: str | Path, frontend_dir: str | Path = FRONTEND_DATA_DIR) -> None:
    source = Path(source_dir)
    target = Path(frontend_dir)
    target.mkdir(parents=True, exist_ok=True)
    for filename in ("analytics.json", "customer_segments.json", "products.json", "recommendations.json", "evaluation.json"):
        source_file = source / filename
        if source_file.exists() and source_file.resolve() != (target / filename).resolve():
            shutil.copy2(source_file, target / filename)
