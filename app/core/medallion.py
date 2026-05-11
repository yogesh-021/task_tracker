from datetime import datetime, timezone

import pandas as pd

VALID_LAYERS = {"bronze", "silver", "gold"}


# ──────────────────────────────────────────────
# BRONZE LAYER
# Goal: load raw data and record when it arrived.
# No changes to the actual values.
# ──────────────────────────────────────────────

def apply_bronze(df: pd.DataFrame) -> dict:
    df = df.copy()

    # Stamp every row with the time it was ingested
    df["_ingest_timestamp"] = datetime.now(timezone.utc).isoformat()

    return {
        "layer": "bronze",
        "row_count": len(df),
        "column_count": len(df.columns),
        "ingest_timestamp": df["_ingest_timestamp"].iloc[0],
        "data": df.to_dict(orient="records"),
    }


# ──────────────────────────────────────────────
# SILVER LAYER
# Goal: clean up the data so it is ready to use.
#   1. Remove duplicate rows
#   2. Fill in missing values
#   3. Trim whitespace and lowercase text columns
# ──────────────────────────────────────────────

def apply_silver(df: pd.DataFrame) -> dict:
    df = df.copy()

    # Step 1 – drop exact duplicate rows
    rows_before = len(df)
    df = df.drop_duplicates()
    duplicates_removed = rows_before - len(df)

    # Step 2 – fill missing values
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Use the column average for numbers
            df[col] = df[col].fillna(df[col].mean())
        else:
            # Use "Unknown" for text columns
            df[col] = df[col].fillna("Unknown")

    # Step 3 – clean up text columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.lower()

    df["_silver_processed_at"] = datetime.now(timezone.utc).isoformat()

    return {
        "layer": "silver",
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicates_removed": duplicates_removed,
        "data": df.to_dict(orient="records"),
    }


# ──────────────────────────────────────────────
# GOLD LAYER
# Goal: produce simple summaries / aggregations
# that are ready for dashboards or reports.
# ──────────────────────────────────────────────

def apply_gold(df: pd.DataFrame) -> dict:
    # Start from silver-cleaned data
    silver_result = apply_silver(df)
    df_clean = pd.DataFrame(silver_result["data"])

    # Remove the audit columns added by silver
    audit_cols = [col for col in df_clean.columns if col.startswith("_")]
    df_clean = df_clean.drop(columns=audit_cols)

    # Basic stats for each numeric column
    numeric_summary = {}
    for col in df_clean.select_dtypes(include="number").columns:
        numeric_summary[col] = {
            "count": int(df_clean[col].count()),
            "mean":  round(float(df_clean[col].mean()), 2),
            "min":   float(df_clean[col].min()),
            "max":   float(df_clean[col].max()),
            "sum":   round(float(df_clean[col].sum()), 2),
        }

    # Top values for each text column
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
        "data": df_clean.to_dict(orient="records"),
    }


# ──────────────────────────────────────────────
# DISPATCHER
# Maps a layer name to the right function.
# ──────────────────────────────────────────────

_LAYER_FN = {
    "bronze": apply_bronze,
    "silver": apply_silver,
    "gold":   apply_gold,
}


def transform(file_path: str, layer: str) -> dict:
    df = pd.read_csv(file_path)
    return _LAYER_FN[layer](df)
