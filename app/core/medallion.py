import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

VALID_LAYERS = {"bronze", "silver", "gold"}


def _safe_records(df: pd.DataFrame) -> list:
    """Convert DataFrame to records — NaN/inf → null, numpy types → Python native."""
    cleaned = df.replace([np.inf, -np.inf], np.nan)
    return json.loads(cleaned.to_json(orient="records"))

def apply_bronze(df: pd.DataFrame) -> dict:
    df = df.copy()

    df["_ingest_timestamp"] = datetime.now(timezone.utc).isoformat()

    return {
        "layer": "bronze",
        "row_count": len(df),
        "column_count": len(df.columns),
        "ingest_timestamp": df["_ingest_timestamp"].iloc[0],
        "data": _safe_records(df),
    }


def apply_silver(df: pd.DataFrame) -> dict:
    df = df.copy()

    rows_before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = rows_before - len(df)

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            mean_val = df[col].mean()
            df[col] = df[col].fillna(mean_val if pd.notna(mean_val) else 0)
        else:
            df[col] = df[col].fillna("Unknown")


    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.lower()

    df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

    return {
        "layer": "silver",
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicates_removed": duplicates_removed,
        "data": _safe_records(df),
    }


def apply_gold(df: pd.DataFrame) -> dict:
    
    silver_result = apply_silver(df)
    df_clean = pd.DataFrame(silver_result["data"])

    audit_cols = [col for col in df_clean.columns if col.startswith("_")]
    df_clean = df_clean.drop(columns=audit_cols)

    numeric_summary = {}
    for col in df_clean.select_dtypes(include="number").columns:
        numeric_summary[col] = {
            "count": int(df_clean[col].count()),
            "mean":  round(float(df_clean[col].mean()), 2),
            "min":   float(df_clean[col].min()),
            "max":   float(df_clean[col].max()),
            "sum":   round(float(df_clean[col].sum()), 2),
        }

    text_summary = {}
    for col in df_clean.select_dtypes(include="object").columns:
        top_values = df_clean[col].value_counts().head(5).to_dict()
        text_summary[col] = {
            "unique_values": int(df_clean[col].nunique()),
            "top_5": {str(k): int(v) for k, v in top_values.items()},
        }

    df_clean["_gold_processed_at"] = datetime.now(timezone.utc).isoformat()

    return {
        "layer": "gold",
        "row_count": len(df_clean),
        "numeric_summary": numeric_summary,
        "text_summary": text_summary,
        "data": _safe_records(df_clean),
    }


_LAYER_FN = {
    "bronze": apply_bronze,
    "silver": apply_silver,
    "gold":   apply_gold,
}


def transform(file_path: str, layer: str) -> dict:
    df = pd.read_csv(file_path)
    return _LAYER_FN[layer](df)
