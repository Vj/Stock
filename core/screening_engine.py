import pandas as pd


class ScreeningEngine:
    def __init__(self, df):
        self.df = df

    def apply_filters(self, rules):
        filtered = self.df[
            (self.df["pe"] < rules.get("pe", 25)) &
            (self.df["roe"] > rules.get("roe", 15)) &
            (self.df["roce"] > rules.get("roce", 15)) &
            (self.df["debt_equity"] < rules.get("debt_equity", 0.5))
        ]
        return filtered
