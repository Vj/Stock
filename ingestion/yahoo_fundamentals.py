"""Yahoo-based fundamentals ingestion with caching."""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List

import pandas as pd
import yfinance as yf

from utils.logger import get_logger


logger = get_logger(__name__)


def _safe_get(series, key):
    return series.get(key) if series is not None else None


def fetch_fundamentals(symbols: Iterable[str]) -> pd.DataFrame:
    rows: List[dict] = []
    for symbol in symbols:
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            row = {
                "symbol": symbol,
                "pe": _safe_get(info, "trailingPE"),
                "roe": _safe_get(info, "returnOnEquity"),
                "roce": _safe_get(info, "returnOnAssets"),
                "debt_equity": _safe_get(info, "debtToEquity"),
                "revenue_growth": _safe_get(info, "revenueGrowth"),
                "profit_growth": _safe_get(info, "earningsGrowth"),
            }
            rows.append(row)
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", symbol, exc)

    df = pd.DataFrame(rows)
    for col in ["roe", "roce", "revenue_growth", "profit_growth"]:
        if col in df.columns:
            df[col] = df[col] * 100
    return df


def cache_fundamentals_csv(df: pd.DataFrame, out_dir="data/raw") -> str:
    import os

    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"fundamentals_{stamp}.csv")
    df.to_csv(path, index=False)
    return path
