import pandas as pd


class ScoringEngine:
    def __init__(self, df):
        self.df = df

    def calculate_score(self):
        self.df["score"] = (
            self.df["roe"] * 0.3
            + self.df["roce"] * 0.3
            + self.df["revenue_growth"] * 0.2
            + self.df["profit_growth"] * 0.2
        )
        return self.df
