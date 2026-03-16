import os
import duckdb
import pandas as pd


def cache_fundamentals_duckdb(df: pd.DataFrame, db_path="data/processed/fundamentals.duckdb") -> str:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals (
            symbol TEXT,
            pe DOUBLE,
            roe DOUBLE,
            roce DOUBLE,
            debt_equity DOUBLE,
            revenue_growth DOUBLE,
            profit_growth DOUBLE,
            fetched_at TIMESTAMP
        )
        """
    )
    df = df.copy()
    df["fetched_at"] = pd.Timestamp.utcnow()
    con.register("df", df)
    con.execute("INSERT INTO fundamentals SELECT * FROM df")
    con.close()
    return db_path


def load_latest_fundamentals(db_path="data/processed/fundamentals.duckdb") -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    con = duckdb.connect(db_path)
    df = con.execute(
        """
        SELECT *
        FROM fundamentals
        QUALIFY fetched_at = max(fetched_at) OVER ()
        """
    ).fetchdf()
    con.close()
    return df
