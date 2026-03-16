"""Simple pipeline runner to produce exports/top_stocks.csv."""
import argparse
import os
import pandas as pd
import yaml

from core.screening_engine import ScreeningEngine
from core.scoring_engine import ScoringEngine
from ingestion.yahoo_fundamentals import fetch_fundamentals, cache_fundamentals_csv
from ingestion.nse_fetcher import fetch_nse_quote
from ingestion.screener_fetcher import fetch_screener_snapshot
from utils.cache import cache_fundamentals_duckdb, load_latest_fundamentals
from utils.data_formatter import format_stock_table
from utils.logger import get_logger


logger = get_logger(__name__)


def load_config(path="config/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def enrich_with_nse(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for symbol in df["symbol"].tolist():
        try:
            rows.append(fetch_nse_quote(symbol))
        except Exception as exc:
            logger.warning("NSE fetch failed for %s: %s", symbol, exc)
    if not rows:
        return df
    nse_df = pd.concat(rows, ignore_index=True)
    return df.merge(nse_df, on="symbol", how="left")


def enrich_with_screener(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for symbol in df["symbol"].tolist():
        try:
            rows.append(fetch_screener_snapshot(symbol))
        except Exception as exc:
            logger.warning("Screener fetch failed for %s: %s", symbol, exc)
    if not rows:
        return df
    screener_df = pd.concat(rows, ignore_index=True)
    return df, screener_df


def run_pipeline(symbols, config_path="config/config.yaml", use_cache=True):
    cfg = load_config(config_path)
    rules = cfg.get("screening_rules", {})

    df = fetch_fundamentals(symbols)
    if df.empty:
        if use_cache:
            cached = load_latest_fundamentals()
            if cached.empty:
                raise RuntimeError("No fundamentals fetched and no cache available")
            df = cached
            logger.info("Using cached fundamentals")
        else:
            raise RuntimeError("No fundamentals fetched")

    cache_fundamentals_csv(df)
    cache_fundamentals_duckdb(df)

    df = enrich_with_nse(df)
    screener_df = None
    try:
        df, screener_df = enrich_with_screener(df)
    except Exception:
        pass

    screened = ScreeningEngine(df).apply_filters(rules)
    scored = ScoringEngine(screened).calculate_score()
    formatted = format_stock_table(scored)

    os.makedirs(os.path.join("data", "exports"), exist_ok=True)
    out_path = os.path.join("data", "exports", "top_stocks.csv")
    formatted.to_csv(out_path, index=False)

    if screener_df is not None:
        screener_out = os.path.join("data", "exports", "screener_snapshot.csv")
        screener_df.to_csv(screener_out, index=False)

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Run daily stock pipeline")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["RELIANCE.NS", "TCS.NS", "INFY.NS"],
        help="Ticker symbols to ingest",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config YAML",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache fallback",
    )
    args = parser.parse_args()

    path = run_pipeline(args.symbols, args.config, use_cache=not args.no_cache)
    print(f"Exported: {path}")


if __name__ == "__main__":
    main()
