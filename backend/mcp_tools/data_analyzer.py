"""
MCP Tool — Data Analyzer
Handles: descriptive stats, trend analysis, correlations,
         top-performers, sentiment detection, feature engineering.
Returns a rich analysis dict ready for the LLM and frontend.
"""
import pandas as pd
import numpy as np
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _detect_revenue_col(df: pd.DataFrame) -> str | None:
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    priorities = [
        ["revenue"],
        ["total", "amount", "sales", "sale", "income"],
        ["price", "value"]
    ]
    for p_list in priorities:
        for c in numeric:
            if any(k in c.lower() for k in p_list):
                return c
    return numeric[0] if numeric else None


def _detect_date_col(df: pd.DataFrame) -> str | None:
    # 1. Prefer the synthetic column injected by data_cleaner
    if "__date__" in df.columns and pd.api.types.is_datetime64_any_dtype(df["__date__"]):
        return "__date__"

    # 2. Any column already parsed as datetime
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            return c

    # 3. String column whose name suggests a date and that parses cleanly
    for c in df.columns:
        if any(k in c.lower() for k in ["date", "time", "period"]):
            try:
                parsed = pd.to_datetime(df[c], errors="coerce")
                if parsed.notna().sum() / max(len(df), 1) > 0.5:
                    df[c] = parsed      # coerce in-place so downstream code works
                    return c
            except Exception:
                pass

    # 4. Reconstruct from integer year / month / day columns
    _year_col  = next((c for c in df.columns if c.lower() == "year"),  None)
    _month_col = next((c for c in df.columns if c.lower() == "month"), None)
    _day_col   = next((c for c in df.columns if c.lower() == "day"),   None)
    if _year_col and _month_col:
        try:
            _day_s = df[_day_col] if _day_col else 1
            reconstructed = pd.to_datetime(
                dict(year=df[_year_col], month=df[_month_col], day=_day_s),
                errors="coerce"
            )
            if reconstructed.notna().sum() / max(len(df), 1) > 0.5:
                df["__date__"] = reconstructed
                return "__date__"
        except Exception:
            pass

    return None


def _detect_category_col(df: pd.DataFrame) -> str | None:
    for c in df.select_dtypes(include="object").columns:
        if any(k in c.lower() for k in ["product", "item", "category", "name", "sku", "type", "segment", "region", "channel"]):
            return c
    obj = df.select_dtypes(include="object").columns.tolist()
    return obj[0] if obj else None


def _detect_sentiment_col(df: pd.DataFrame) -> str | None:
    for c in df.select_dtypes(include="object").columns:
        if any(k in c.lower() for k in ["review", "comment", "feedback", "text", "sentiment", "opinion", "rating"]):
            return c
    return None


def _simple_sentiment(text: str) -> str:
    """Keyword-based sentiment (no NLTK dependency)."""
    if not isinstance(text, str):
        return "neutral"
    text_lower = text.lower()
    pos = ["great", "excellent", "good", "amazing", "love", "best", "perfect", "awesome", "fantastic", "happy"]
    neg = ["bad", "terrible", "worst", "hate", "awful", "poor", "horrible", "disappoint", "broken", "slow", "refund"]
    pos_score = sum(1 for w in pos if w in text_lower)
    neg_score = sum(1 for w in neg if w in text_lower)
    if pos_score > neg_score:
        return "positive"
    if neg_score > pos_score:
        return "negative"
    return "neutral"


# ─────────────────────────────────────────────────────────────
# MAIN TOOL
# ─────────────────────────────────────────────────────────────

def analyze_data(df: pd.DataFrame) -> dict:
    """
    MCP Tool Call: Statistical Analysis Pipeline.

    Sections:
        1. Schema overview
        2. Descriptive statistics
        3. Revenue / Sales KPIs
        4. Trend over time
        5. Top & bottom performers (by category)
        6. Correlation matrix
        7. Sentiment analysis (if text col exists)
        8. Data quality score
        9. Summary text for LLM

    Returns:
        analysis dict
    """
    result = {
        "tool": "data_analyzer",
        "timestamp": datetime.utcnow().isoformat(),
        "schema": {},
        "descriptive": {},
        "kpis": {},
        "trend": {},
        "performers": {},
        "correlation": {},
        "sentiment": {},
        "quality_score": 0,
        "summary_text": ""
    }

    if df.empty:
        result["summary_text"] = "Dataset is empty."
        return result

    # ── 1. Schema ─────────────────────────────────────────────
    result["schema"] = {
        "rows": len(df),
        "cols": len(df.columns),
        "columns": [
            {
                "name": c,
                "dtype": str(df[c].dtype),
                "nulls": int(df[c].isnull().sum()),
                "unique": int(df[c].nunique())
            }
            for c in df.columns
        ]
    }

    # ── 2. Descriptive statistics ─────────────────────────────
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        desc = numeric_df.describe().to_dict()
        # Convert numpy types → native Python
        result["descriptive"] = {
            col: {k: (float(v) if pd.notna(v) else None) for k, v in stats.items()}
            for col, stats in desc.items()
        }
        # Skewness & Kurtosis
        for col in numeric_df.columns:
            if col in result["descriptive"]:
                result["descriptive"][col]["skewness"] = round(float(numeric_df[col].skew()), 4)
                result["descriptive"][col]["kurtosis"] = round(float(numeric_df[col].kurt()), 4)

    # ── 3. Revenue / Sales KPIs ───────────────────────────────
    rev_col = _detect_revenue_col(df)
    if rev_col:
        s = df[rev_col].dropna()
        result["kpis"] = {
            "revenue_column": rev_col,
            "total": round(float(s.sum()), 2),
            "mean": round(float(s.mean()), 2),
            "median": round(float(s.median()), 2),
            "std": round(float(s.std()), 2),
            "min": round(float(s.min()), 2),
            "max": round(float(s.max()), 2),
            "total_records": len(df),
            "non_null_revenue": int(s.count()),
            "growth_rate_pct": None  # filled if trend available
        }

    # ── 4. Trend over time ────────────────────────────────────
    date_col = _detect_date_col(df)
    if date_col and rev_col:
        try:
            temp = df[[date_col, rev_col]].copy()
            # Only convert if not already a datetime column (e.g. __date__ from cleaner)
            if not pd.api.types.is_datetime64_any_dtype(temp[date_col]):
                temp[date_col] = pd.to_datetime(temp[date_col], errors="coerce")
            temp = temp.dropna(subset=[date_col, rev_col])
            
            # Robust generic aggregation step
            monthly = temp.groupby(temp[date_col].dt.to_period('M'))[rev_col].sum()
            monthly = monthly.sort_index()

            print("=======================================")
            print("MANDATORY VALIDATION: Monthly Revenue")
            print(monthly)
            print("=======================================")

            if monthly.empty or monthly.sum() == 0:
                print("WARNING: Aggregation resulted in zero or empty data!")
            else:
                first_month = temp[date_col].dt.to_period('M').min()
                subset = temp[temp[date_col].dt.to_period('M') == first_month]
                print("First month rows:", len(subset))
                print("First month revenue:", subset[rev_col].sum())
                print("=======================================")

            if len(monthly) > 0:
                # dt.to_period('M') index can be converted to timestamp for formatting
                labels = [d.to_timestamp().strftime("%b %Y") for d in monthly.index]
                values = [round(float(v), 2) for v in monthly.values]
                
                # Ensure complete data usage validation
                assert abs(monthly.sum() - temp[rev_col].sum()) < 0.1, "Mismatch in total revenue aggregation"

                # Trend interpretation and Growth Rate
                trend_label = "insufficient data"
                if len(values) >= 3:
                    v1, v2, v3 = values[0], values[1], values[2]
                    print("First Month:", v1)
                    print("Second Month:", v2)
                    print("Third Month:", v3)
                    
                    if v2 < v1 and v3 > v2:
                        trend_label = "decline followed by recovery"
                    elif v2 > v1 and v3 < v2:
                        trend_label = "growth followed by decline"
                    elif v3 > v2 > v1:
                        trend_label = "consistent growth"
                    elif v3 < v2 < v1:
                        trend_label = "consistent decline"
                    else:
                        trend_label = "fluctuating trend"
                elif len(values) == 2:
                    trend_label = "growth" if values[1] > values[0] else "decline"

                if len(values) >= 2 and values[0] != 0:
                    growth = round(((values[-1] - values[0]) / values[0]) * 100, 2)
                    if result["kpis"]:
                        result["kpis"]["growth_rate_pct"] = growth
                        result["kpis"]["trend_interpretation"] = trend_label

                result["trend"] = {
                    "date_column": date_col,
                    "frequency": "monthly",
                    "labels": labels,
                    "values": values,
                    "peak_period": labels[int(np.argmax(values))] if values else None,
                    "peak_value": max(values) if values else None,
                    "trend_label": trend_label
                }
        except Exception as e:
            result["trend"] = {"error": str(e)}

    # Fallback trend — use first numeric col row order
    if not result["trend"] and rev_col:
        vals = df[rev_col].dropna().head(24).tolist()
        result["trend"] = {
            "date_column": None,
            "frequency": "row",
            "labels": [f"Row {i+1}" for i in range(len(vals))],
            "values": [round(float(v), 2) for v in vals]
        }

    # ── 5. Top & Bottom Performers ────────────────────────────
    cat_col = _detect_category_col(df)
    if cat_col and rev_col:
        grouped = df.groupby(cat_col)[rev_col].sum().sort_values(ascending=False)
        top5 = grouped.head(5)
        bot5 = grouped.tail(5)
        result["performers"] = {
            "category_column": cat_col,
            "top": [{"name": str(k), "value": round(float(v), 2)} for k, v in top5.items()],
            "bottom": [{"name": str(k), "value": round(float(v), 2)} for k, v in bot5.items()],
            "total_categories": int(grouped.shape[0])
        }

    # ── 6. Correlation matrix ─────────────────────────────────
    if len(numeric_df.columns) >= 2:
        corr = numeric_df.corr(method="pearson").round(4)
        corr_dict = {}
        for col in corr.columns:
            corr_dict[col] = {
                other: (float(corr.loc[col, other]) if pd.notna(corr.loc[col, other]) else None)
                for other in corr.columns
                if other != col
            }
        result["correlation"] = corr_dict

    # ── 7. Sentiment analysis ─────────────────────────────────
    sent_col = _detect_sentiment_col(df)
    if sent_col:
        sentiments = df[sent_col].dropna().astype(str).apply(_simple_sentiment)
        counts = sentiments.value_counts().to_dict()
        total = len(sentiments)
        result["sentiment"] = {
            "column": sent_col,
            "distribution": {
                "positive": int(counts.get("positive", 0)),
                "neutral": int(counts.get("neutral", 0)),
                "negative": int(counts.get("negative", 0))
            },
            "positive_pct": round(counts.get("positive", 0) / max(total, 1) * 100, 1),
            "negative_pct": round(counts.get("negative", 0) / max(total, 1) * 100, 1),
            "total_analyzed": total
        }

    # ── 8. Data quality score (0–100) ─────────────────────────
    completeness = (1 - df.isnull().mean().mean()) * 100
    uniqueness = min(df.duplicated().sum() / max(len(df), 1) * 100, 100)
    quality = round((completeness + (100 - uniqueness)) / 2, 1)
    result["quality_score"] = quality

    # ── 9. Summary text for LLM ctx ───────────────────────────
    lines = [f"Dataset: {len(df)} rows × {len(df.columns)} columns. Data quality: {quality}/100."]
    if rev_col and result["kpis"]:
        k = result["kpis"]
        lines.append(f"Revenue column: '{rev_col}'. Total: ${k['total']:,.2f}, Mean: ${k['mean']:,.2f}, Max: ${k['max']:,.2f}.")
        if k["growth_rate_pct"] is not None:
            trend_desc = k.get("trend_interpretation", "unknown trend")
            lines.append(f"Overall growth rate (first→last period): {k['growth_rate_pct']}%. The general trend shows a {trend_desc}.")
    if result["performers"]:
        p = result["performers"]
        top_names = ", ".join(x["name"] for x in p["top"][:3])
        lines.append(f"Top 3 by '{p['category_column']}': {top_names}.")
    if result["trend"].get("peak_period"):
        lines.append(f"Peak period: {result['trend']['peak_period']} (${result['trend']['peak_value']:,.2f}).")
    if result["sentiment"]:
        s = result["sentiment"]
        lines.append(
            f"Sentiment on '{s['column']}': {s['positive_pct']}% positive, "
            f"{s['negative_pct']}% negative ({s['total_analyzed']} records analyzed)."
        )
    result["summary_text"] = " ".join(lines)

    return result
