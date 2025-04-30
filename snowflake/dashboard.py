import streamlit as st
import pandas as pd
import snowflake.connector
import altair as alt
import os
from dotenv import load_dotenv
from pathlib import Path
import time


env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

st.set_page_config(
    page_title="TfL Transport Dashboard",
    page_icon="🚌",
    layout="wide",
)




user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
role = os.getenv("SNOWFLAKE_ROLE")

# --- Connect to Snowflake ---
@st.cache_resource
def create_connection():
    con = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse = warehouse,
    database=database,
    schema=schema,
    role=role,
    session_parameters={
        'QUERY_TAG': 'TflOpenDATADashboard',
        }
    )
    return con

conn = create_connection()

# --- Helper to run query ---
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall(), [desc[0] for desc in cur.description]

# --- Page config ---


st.title("🚇 Transport for London - Live & Historical Insights")

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs(["Next Arrivals", "Busiest Days", "Busiest Stops (1hr)", "Busiest Stops (7d)"])

# 1. Next Arrivals
with tab1:
    st.subheader("🚏 Next Scheduled Arrivals (Live)")
    data, cols = run_query("SELECT * FROM next_arrivals;")
    df_next_arrivals = pd.DataFrame(data, columns=cols)
    
    if not df_next_arrivals.empty:
        st.dataframe(df_next_arrivals, use_container_width=True)
    else:
        st.info("No upcoming arrivals found.")

# 2. Busiest Service Days
with tab2:
    st.subheader("📅 Busiest Service Days")
    data, cols = run_query("SELECT * FROM busiest_service_days;")
    df_service_days = pd.DataFrame(data, columns=cols)
    print(df_service_days.columns)
    df_service_days.columns = df_service_days.columns.str.lower()

    if not df_service_days.empty:
        chart = alt.Chart(df_service_days).mark_bar().encode(
            x=alt.X('day_of_week', sort=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']),
            y='scheduled_services:Q',
            color='day_of_week:N',
            tooltip=['day_of_week', 'scheduled_services']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No service day data available.")

# 3. Busiest Stops - Last 1 Hour
with tab3:
    st.subheader("🕐 Busiest Stops (Past 1 Hour)")
    data, cols = run_query("SELECT * FROM busiest_stops_1hour;")
    df_busiest_1hr = pd.DataFrame(data, columns=cols)
    df_busiest_1hr.columns = df_busiest_1hr.columns.str.lower()
    # print(df_busiest_1hr["stop_point_id"].dtypes)
    # print(df_busiest_1hr['stop_point_id'].isnull().sum())
    df_busiest_1hr['stop_point_id'] = df_busiest_1hr['stop_point_id'].astype(str)
    if not df_busiest_1hr.empty:
        chart = alt.Chart(df_busiest_1hr).mark_bar().encode(
            x='arrivals_count',
            y=alt.Y('stop_point_id', sort='-x'),
            tooltip=['stop_point_id', 'arrivals_count']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No arrival data from the past hour.")

# 4. Busiest Stops - Last 7 Days
with tab4:
    st.subheader("📈 Busiest Stops (Past 7 Days)")
    data, cols = run_query("SELECT * FROM busiest_stops_7days;")
    df_busiest_7d = pd.DataFrame(data, columns=cols)
    df_busiest_7d.columns = df_busiest_7d.columns.str.lower()
    df_busiest_7d['stop_point_id'] = df_busiest_7d['stop_point_id'].astype(str)
    # print(df_busiest_7d.columns)
    if not df_busiest_7d.empty:
        chart = alt.Chart(df_busiest_7d).mark_bar().encode(
            x='total_arrivals_7d:Q',
            y=alt.Y('stop_point_id:N', sort='-x'),
            tooltip=['stop_point_id', 'total_arrivals_7d']
        ).properties(width=700, height=400)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No arrival data for the past 7 days.")
