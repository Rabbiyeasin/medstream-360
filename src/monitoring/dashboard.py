import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="MedStream 360 Dashboard", layout="wide")

con = duckdb.connect("dev.duckdb")

st.title("🏥 MedStream 360 - Healthcare Pipeline Monitor")

# -------------------------
# SILVER DATA
# -------------------------
st.header("🧼 Silver Vitals")

df_silver = con.execute("""
SELECT *
FROM main_silver.silver_vitals
LIMIT 100
""").df()

st.dataframe(df_silver)

# -------------------------
# HIGH RISK ALERTS
# -------------------------
st.header("🚨 High Risk Alerts")

df_alerts = con.execute("""
SELECT *
FROM main_gold.gold_high_risk_alerts
ORDER BY subject_id
LIMIT 100
""").df()

st.dataframe(df_alerts)

# -------------------------
# DAILY SUMMARY
# -------------------------
st.header("📊 Patient Daily Summary")

df_summary = con.execute("""
SELECT *
FROM main_gold.gold_patient_daily_summary
ORDER BY observation_date DESC
LIMIT 100
""").df()

st.dataframe(df_summary)

# -------------------------
# METRICS
# -------------------------
st.header("📈 Key Metrics")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Silver Records", len(df_silver))

with col2:
    st.metric("Total Alerts", len(df_alerts))

with col3:
    st.metric("Unique Patients", df_silver["subject_id"].nunique())