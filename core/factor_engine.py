import pandas as pd


class FactorEngine:
    def __init__(self, df):
        self.df = df

    def calculate_momentum(self):
        self.df["dma50"] = self.df["Close"].rolling(50).mean()
        self.df["dma200"] = self.df["Close"].rolling(200).mean()
        return self.df

    def calculate_returns(self):
        self.df["1y_return"] = self.df["Close"].pct_change(252)
        return self.df
