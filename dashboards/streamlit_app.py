import streamlit as st
import pandas as pd
import os

from scheduler.daily_pipeline import run_pipeline


st.title("Stock Research Dashboard")

csv_path = os.path.join("data", "exports", "top_stocks.csv")
if not os.path.exists(csv_path):
    st.info("CSV not found. Running pipeline...")
    run_pipeline(["RELIANCE.NS", "TCS.NS", "INFY.NS"], use_cache=True)

if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
    st.dataframe(df)
else:
    st.error("CSV not generated.")
