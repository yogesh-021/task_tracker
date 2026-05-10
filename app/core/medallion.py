import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

VALID_LAYERS = {"bronze", "silver", "gold"}


# ---------- helpers ----------

def _to_native(obj):
    """Recursively convert numpy scalar types to Python native types for JSON safety."""
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(i) for i in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


def _load_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


# ---------- Bronze ----------
# Preserve raw data as-is; append audit columns for lineage tracking.

def apply_bronze(df: pd.DataFrame) -> dict:
    df = df.copy()
    source_schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    df["_source_schema"] = json.dumps(source_schema)
    df["_ingest_timestamp"] = datetime.now(timezone.utc).isoformat()

    return _to_native({
        "layer": "bronze",
        "row_count": len(df),
        "column_count": len(df.columns),
        "source_schema": source_schema,
        "ingest_timestamp": df["_ingest_timestamp"].iloc[0],
        "data": df.to_dict(orient="records"),
    })


# ---------- Silver ----------
# Clean, validate, run DQ checks, apply basic standardisation.

def apply_silver(df: pd.DataFrame) -> dict:
    df = df.copy()

    original_rows = len(df)
    null_counts_before = df.isnull().sum().to_dict()

    # 1. Remove exact duplicates
    df = df.drop_duplicates()
    duplicates_removed = original_rows - len(df)

    # 2. Fill nulls — median for numerics, mode (or 'Unknown') for strings
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            fill_val = df[col].median()
            df[col] = df[col].fillna(0 if pd.isna(fill_val) else fill_val)
        else:
            mode = df[col].mode()
            df[col] = df[col].fillna(mode.iloc[0] if not mode.empty else "Unknown")

    # 3. Standardise string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.lower()

    # 4. DQ report
    completeness_pct = {
        col: round((1 - df[col].isnull().mean()) * 100, 2)
        for col in df.columns
    }
    uniqueness_pct = {
        col: round(df[col].nunique() / len(df) * 100, 2) if len(df) > 0 else 100.0
        for col in df.columns
    }
    type_consistency = {
        col: str(df[col].dtype)
        for col in df.columns
    }

    df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

    return _to_native({
        "layer": "silver",
        "row_count": len(df),
        "column_count": len(df.columns),
        "dq_report": {
            "original_row_count": original_rows,
            "cleaned_row_count": len(df),
            "duplicates_removed": duplicates_removed,
            "null_counts_before": null_counts_before,
            "completeness_pct": completeness_pct,
            "uniqueness_pct": uniqueness_pct,
            "type_consistency": type_consistency,
        },
        "data": df.to_dict(orient="records"),
    })


# ---------- Gold ----------
# Business KPIs derived from silver-quality data.

def apply_gold(df: pd.DataFrame) -> dict:
    # Start from clean silver data
    silver = apply_silver(df)
    df_clean = pd.DataFrame(silver["data"])

    # Drop audit columns added by silver
    audit_cols = [c for c in df_clean.columns if c.startswith("_")]
    df_clean = df_clean.drop(columns=audit_cols, errors="ignore")

    numeric_cols = df_clean.select_dtypes(include="number").columns.tolist()
    cat_cols = df_clean.select_dtypes(include="object").columns.tolist()

    # Numeric KPIs per column
    numeric_kpis: dict = {}
    for col in numeric_cols:
        s = df_clean[col]
        numeric_kpis[col] = {
            "count": int(s.count()),
            "sum": float(round(s.sum(), 4)),
            "mean": float(round(s.mean(), 4)),
            "median": float(round(s.median(), 4)),
            "std": float(round(s.std(), 4)) if len(s) > 1 else 0.0,
            "min": float(s.min()),
            "max": float(s.max()),
        }

    # Categorical KPIs per column
    categorical_kpis: dict = {}
    for col in cat_cols:
        vc = df_clean[col].value_counts()
        categorical_kpis[col] = {
            "unique_count": int(df_clean[col].nunique()),
            "top_5": {str(k): int(v) for k, v in vc.head(5).items()},
            "null_count": int(df_clean[col].isnull().sum()),
        }

    df_clean["_gold_processed_at"] = datetime.now(timezone.utc).isoformat()

    return _to_native({
        "layer": "gold",
        "row_count": len(df_clean),
        "kpis": {
            "dataset_summary": {
                "total_rows": len(df_clean),
                "total_columns": len(df_clean.columns),
                "numeric_columns": numeric_cols,
                "categorical_columns": cat_cols,
            },
            "numeric_kpis": numeric_kpis,
            "categorical_kpis": categorical_kpis,
        },
        "data": df_clean.to_dict(orient="records"),
    })


# ---------- Dispatcher ----------

_LAYER_FN = {
    "bronze": apply_bronze,
    "silver": apply_silver,
    "gold": apply_gold,
}


def transform(file_path: str, layer: str) -> dict:
    df = _load_csv(file_path)
    return _LAYER_FN[layer](df)
