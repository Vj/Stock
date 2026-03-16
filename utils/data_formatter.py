def format_stock_table(df):
    columns = [
        "symbol",
        "score",
        "roe",
        "roce",
        "pe",
        "revenue_growth",
    ]
    return df[columns].sort_values(by="score", ascending=False)
