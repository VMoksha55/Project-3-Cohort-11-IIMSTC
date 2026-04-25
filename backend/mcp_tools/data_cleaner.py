"""
MCP Tool — Data Cleaner
Handles: null imputation, duplicate removal, dtype coercion, outlier capping.
Returns a cleaned DataFrame + a cleaning report dict.
"""
import pandas as pd
import numpy as np
from datetime import datetime


def clean_data(filepath: str) -> tuple[pd.DataFrame, dict]:
    """
    MCP Tool Call: Data Cleaning Pipeline.

    Steps:
        1. Load CSV
        2. Remove duplicate rows
        3. Strip whitespace from string columns
        4. Coerce numeric columns (remove $, commas)
        5. Parse date columns
        6. Impute missing values (median for numeric, mode for categorical)
        7. Cap outliers with IQR method
        8. Reset index

    Returns:
        (cleaned_df, report_dict)
    """
    report = {
        "tool": "data_cleaner",
        "timestamp": datetime.utcnow().isoformat(),
        "steps": [],
        "original_shape": None,
        "clean_shape": None,
        "issues_fixed": 0
    }

    # ── Step 1: Load ──────────────────────────────────────────
    df = pd.read_csv(filepath, encoding="utf-8", encoding_errors="replace")
    report["original_shape"] = {"rows": len(df), "cols": len(df.columns)}
    report["columns"] = df.columns.tolist()

    # ── Step 2: Remove duplicates ─────────────────────────────
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    report["steps"].append({"step": "remove_duplicates", "removed_rows": removed})
    report["issues_fixed"] += removed

    # ── Step 3: Strip whitespace from string cols ─────────────
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": np.nan, "None": np.nan, "": np.nan})
    report["steps"].append({"step": "strip_whitespace", "columns_processed": len(df.select_dtypes(include="object").columns)})

    # ── Step 4: Coerce numeric columns ───────────────────────
    coerced = []
    for col in df.columns:
        if df[col].dtype == object:
            # Try to clean currency / percentage strings
            test = df[col].dropna().astype(str).str.replace(r"[\$,€£%]", "", regex=True).str.strip()
            try:
                pd.to_numeric(test)
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(r"[\$,€£%]", "", regex=True).str.strip(),
                    errors="coerce"
                )
                coerced.append(col)
            except Exception:
                pass
    report["steps"].append({"step": "coerce_numerics", "columns": coerced})

    # ── Step 5: Parse date columns ────────────────────────────
    date_cols = []
    for col in df.columns:
        if df[col].dtype == object:
            cl = col.lower()
            if any(k in cl for k in ["date", "time", "month", "year", "period", "created", "updated"]):
                parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
                if parsed.notna().sum() / max(len(df), 1) > 0.5:
                    df[col] = parsed
                    date_cols.append(col)
    report["steps"].append({"step": "parse_dates", "columns": date_cols})

    # ── Step 6: Impute missing values ─────────────────────────
    null_before = int(df.isnull().sum().sum())
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())

    for col in df.select_dtypes(include="object").columns:
        if df[col].isnull().any():
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val[0])

    null_after = int(df.isnull().sum().sum())
    report["steps"].append({
        "step": "impute_nulls",
        "nulls_before": null_before,
        "nulls_after": null_after,
        "imputed": null_before - null_after
    })
    report["issues_fixed"] += (null_before - null_after)

    # ── Step 7: Cap outliers (IQR method) ─────────────────────
    outlier_cols = []
    for col in df.select_dtypes(include=[np.number]).columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        if IQR > 0:
            lower = Q1 - 3.0 * IQR
            upper = Q3 + 3.0 * IQR
            n_outliers = int(((df[col] < lower) | (df[col] > upper)).sum())
            if n_outliers > 0:
                df[col] = df[col].clip(lower=lower, upper=upper)
                outlier_cols.append({"col": col, "capped": n_outliers})
                report["issues_fixed"] += n_outliers
    report["steps"].append({"step": "cap_outliers", "columns": outlier_cols})

    # ── Step 8: Reset index ───────────────────────────────────
    df = df.reset_index(drop=True)
    report["clean_shape"] = {"rows": len(df), "cols": len(df.columns)}
    report["steps"].append({"step": "reset_index"})

    return df, report
