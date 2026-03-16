import yfinance as yf
import pandas as pd


def fetch_price_history(symbol):
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5y")
    df.reset_index(inplace=True)
    return df
