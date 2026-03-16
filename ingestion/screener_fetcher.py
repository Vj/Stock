import requests
import pandas as pd


def fetch_screener_snapshot(symbol: str) -> pd.DataFrame:
    """Fetch a basic screener.in snapshot table (public page scrape).

    Screener doesn't provide an open API; this is best-effort HTML scrape.
    """
    url = f"https://www.screener.in/company/{symbol}/"
    res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    res.raise_for_status()

    # Try to read the first table on the page
    tables = pd.read_html(res.text)
    if not tables:
        raise ValueError("No tables found on screener page")

    df = tables[0]
    df.columns = ["metric", "value"]
    df["symbol"] = symbol
    return df
