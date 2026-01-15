import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster, HeatMap
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="York Region Road Safety Events Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 1. Initialize the connection
# This replaces get_db_connection() and handles reconnection/pooling automatically
conn = st.connection("supabase_db", type="sql")

# 2. Simplified Query Function
# Using st.cache_data here is still good for heavy queries
@st.cache_data(ttl=3600)
def run_query(query):
    """Execute a query using the built-in connection."""
    return conn.query(query)

# Main title
st.title("🚗 Road Safety Events Dashboard")
st.markdown("Analysis of road safety events data")

# Sidebar for filters
st.sidebar.header("Filters")

# Get unique values for filters
try:
    municipalities_df = run_query("SELECT DISTINCT municipality FROM road_safety_events WHERE municipality IS NOT NULL ORDER BY municipality")
    municipalities = ["All"] + municipalities_df['municipality'].tolist()
    
    types_df = run_query("SELECT DISTINCT road_safety_occurrence_type FROM road_safety_events WHERE road_safety_occurrence_type IS NOT NULL ORDER BY road_safety_occurrence_type")
    event_types = ["All"] + types_df['road_safety_occurrence_type'].tolist()
except Exception as e:
    st.error(f"Error loading filter options: {e}")
    municipalities = ["All"]
    event_types = ["All"]

selected_municipality = st.sidebar.selectbox("Municipality", municipalities)
selected_type = st.sidebar.selectbox("Event Type", event_types)

# Overview metrics
st.header("📊 Overview")
col1, col2, col3, col4 = st.columns(4)

try:
    # Total events
    total_df = run_query("SELECT COUNT(*) AS total FROM road_safety_events")
    total_events = total_df['total'].iloc[0] if not total_df.empty else 0
    
    # Date range
    date_df = run_query("""
        SELECT 
            MIN(occurrence_date) AS min_date,
            MAX(occurrence_date) AS max_date
        FROM road_safety_events
        WHERE occurrence_date IS NOT NULL
    """)
    
    with col1:
        st.metric("Total Events", f"{total_events:,}")
    
    with col2:
        if not date_df.empty and date_df['min_date'].iloc[0]:
            st.metric("Earliest Date", str(date_df['min_date'].iloc[0])[:10])
    
    with col3:
        if not date_df.empty and date_df['max_date'].iloc[0]:
            st.metric("Latest Date", str(date_df['max_date'].iloc[0])[:10])
    
    with col4:
        muni_count_df = run_query("SELECT COUNT(DISTINCT municipality) AS count FROM road_safety_events WHERE municipality IS NOT NULL")
        unique_municipalities = muni_count_df['count'].iloc[0] if not muni_count_df.empty else 0
        st.metric("Municipalities", unique_municipalities)
        
except Exception as e:
    st.error(f"Error loading overview metrics: {e}")

# Main analysis sections
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "By Municipality", 
    "By Event Type", 
    "Drug/Alcohol Involvement",
    "Temporal Analysis",
    "Raw Data"
])

with tab1:
    st.subheader("Events by Municipality")
    try:
        df = run_query("""
            SELECT
                municipality,
                COUNT(*) AS event_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
            FROM road_safety_events
            WHERE municipality IS NOT NULL
            GROUP BY municipality
            ORDER BY event_count DESC
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(df, x='municipality', y='event_count', title="Event Count by Municipality")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.pie(df, values='event_count', names='municipality', title="Distribution")
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

with tab2:
    st.subheader("Events by Type")
    try:
        df = run_query("""
            SELECT
                road_safety_occurrence_type,
                COUNT(*) AS event_count
            FROM road_safety_events
            WHERE road_safety_occurrence_type IS NOT NULL
            GROUP BY road_safety_occurrence_type
            ORDER BY event_count DESC
        """)
        fig = px.bar(df, x='road_safety_occurrence_type', y='event_count', title="Event Count by Type")
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

with tab3:
    st.subheader("Drug/Alcohol Involvement")
    try:
        df = run_query("""
            SELECT involve_drug_or_alcohol, COUNT(*) AS event_count
            FROM road_safety_events
            WHERE involve_drug_or_alcohol IS NOT NULL
            GROUP BY involve_drug_or_alcohol
        """)
        fig = px.pie(df, values='event_count', names='involve_drug_or_alcohol')
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

with tab4:
    st.subheader("Temporal Analysis")
    try:
        col1, col2 = st.columns(2)
        with col1:
            # Day of week
            df_dow = run_query("""
                SELECT 
                    to_char(occurrence_date, 'Day') as day_name,
                    extract(dow from occurrence_date) as dow,
                    count(*) as event_count
                FROM road_safety_events
                GROUP BY 1, 2 ORDER BY 2
            """)
            st.plotly_chart(px.bar(df_dow, x='day_name', y='event_count', title="By Day"), use_container_width=True)
        
        with col2:
            # Hour of Day
            df_hour = run_query("""
                SELECT extract(hour from time_est) as hour, count(*) as event_count
                FROM road_safety_events WHERE time_est IS NOT NULL
                GROUP BY 1 ORDER BY 1
            """)
            st.plotly_chart(px.bar(df_hour, x='hour', y='event_count', title="By Hour"), use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

with tab5:
    st.subheader("Raw Data")
    limit = st.slider("Rows", 10, 1000, 100)
    df_raw = run_query(f"SELECT * FROM road_safety_events ORDER BY occurrence_date DESC LIMIT {limit}")
    st.dataframe(df_raw, use_container_width=True)

st.markdown("---")

# --- MAP SECTION ---
st.header("🗺️ Geographic Analysis")

# Map-specific filters in a row
map_col1, map_col2, map_col3 = st.columns(3)

with map_col1:
    map_muni = st.selectbox("Map: Filter by Municipality", municipalities, key="map_muni")
with map_col2:
    map_type = st.selectbox("Map: Filter by Event Type", event_types, key="map_type")
with map_col3:
    map_style = st.radio("Map Style", ["Markers", "Heatmap"], horizontal=True)

# Build the dynamic query for the map
map_where_clauses = ["x IS NOT NULL", "y IS NOT NULL"]
if map_muni != "All":
    map_where_clauses.append(f"municipality = '{map_muni}'")
if map_type != "All":
    map_where_clauses.append(f"road_safety_occurrence_type = '{map_type}'")

map_query = f"SELECT x, y, municipality, road_safety_occurrence_type, occurrence_date FROM road_safety_events WHERE {' AND '.join(map_where_clauses)} LIMIT 2000"

try:
    map_df = run_query(map_query)

    if not map_df.empty:
        # Center map based on data (mean of coordinates)
        center_lat = map_df['y'].mean()
        center_lon = map_df['x'].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles="cartodbpositron")
        
        if map_style == "Markers":
            marker_cluster = MarkerCluster().add_to(m)
            for _, row in map_df.iterrows():
                popup_text = f"<b>{row['road_safety_occurrence_type']}</b><br>{row['occurrence_date']}"
                folium.Marker(
                    location=[row['y'], row['x']],
                    popup=folium.Popup(popup_text, max_width=200)
                ).add_to(marker_cluster)
        else:
            # Heatmap expects a list of [lat, lon]
            heat_data = map_df[['y', 'x']].values.tolist()
            HeatMap(heat_data).add_to(m)

        st_folium(m, width="stretch", height=500, key="main_map")
    else:
        st.info("No data found for the selected map filters.")
        
except Exception as e:
    st.error(f"Map Error: {e}")

st.markdown("---")
st.markdown("**Road Safety Events Dashboard**")