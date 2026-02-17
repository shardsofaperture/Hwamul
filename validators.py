from __future__ import annotations

import pandas as pd

from field_specs import validate_table_rows


def require_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    missing = []
    for col in cols:
        if col not in df.columns or df[col].fillna("").astype(str).str.strip().eq("").any():
            missing.append(col)
    return missing


def validate_positive(df: pd.DataFrame, cols: list[str], allow_zero: bool = False) -> list[str]:
    errors = []
    for col in cols:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        bad = vals.lt(0) if allow_zero else vals.le(0)
        if vals.isna().any() or bad.any():
            cmp = ">= 0" if allow_zero else "> 0"
            errors.append(f"{col} must be numeric and {cmp}")
    return errors


def validate_dates(df: pd.DataFrame, cols: list[str]) -> list[str]:
    errors = []
    for col in cols:
        if col not in df.columns:
            continue
        series = df[col].replace("", pd.NA)
        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.isna().any() and series.notna().any():
            errors.append(f"{col} has invalid dates")
    return errors


def validate_with_specs(table_key: str, df: pd.DataFrame) -> list[str]:
    return validate_table_rows(table_key, df)
