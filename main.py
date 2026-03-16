from ingestion.yahoo_fetcher import fetch_price_history
from core.factor_engine import FactorEngine


def run():
    symbol = "RELIANCE.NS"
    df = fetch_price_history(symbol)
    factor = FactorEngine(df)
    df = factor.calculate_momentum()
    print(df.tail())


if __name__ == "__main__":
    run()
