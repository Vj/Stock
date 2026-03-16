import pandas as pd
from core.screening_engine import ScreeningEngine
from core.scoring_engine import ScoringEngine


def test_screening_engine():
    df = pd.DataFrame(
        [
            {"symbol": "A", "pe": 10, "roe": 20, "roce": 20, "debt_equity": 0.2},
            {"symbol": "B", "pe": 30, "roe": 10, "roce": 10, "debt_equity": 1.0},
        ]
    )
    rules = {"pe": 25, "roe": 15, "roce": 15, "debt_equity": 0.5}
    out = ScreeningEngine(df).apply_filters(rules)
    assert len(out) == 1
    assert out.iloc[0]["symbol"] == "A"


def test_scoring_engine():
    df = pd.DataFrame(
        [
            {
                "symbol": "A",
                "roe": 10,
                "roce": 10,
                "revenue_growth": 10,
                "profit_growth": 10,
            }
        ]
    )
    out = ScoringEngine(df).calculate_score()
    assert "score" in out.columns
    assert out.iloc[0]["score"] == 10.0
