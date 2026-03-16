import json
import requests
import pandas as pd


def fetch_nse_quote(symbol: str) -> pd.DataFrame:
    """Fetch NSE quote data via public endpoint.

    Note: NSE blocks some automated requests. This uses a browser-like
    session and may still fail depending on network policy.
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        }
    )

    # Prime cookies
    session.get("https://www.nseindia.com", timeout=10)
    url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
    res = session.get(url, timeout=10)
    res.raise_for_status()
    data = res.json()

    row = {
        "symbol": symbol,
        "lastPrice": data.get("priceInfo", {}).get("lastPrice"),
        "previousClose": data.get("priceInfo", {}).get("previousClose"),
        "open": data.get("priceInfo", {}).get("open"),
        "high": data.get("priceInfo", {}).get("intraDayHighLow", {}).get("max"),
        "low": data.get("priceInfo", {}).get("intraDayHighLow", {}).get("min"),
        "change": data.get("priceInfo", {}).get("change"),
        "pChange": data.get("priceInfo", {}).get("pChange"),
    }
    return pd.DataFrame([row])
