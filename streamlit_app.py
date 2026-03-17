"""
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Streamlit Dashboard for Hands-on Lab

This dashboard demonstrates Snowflake's native Streamlit integration.
No external hosting, no separate deployment - just SQL and Python.
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Pon EV Transition",
    page_icon="🚗",
    layout="wide"
)

session = get_active_session()

st.title("🚗 EV Transition Netherlands")
st.caption("Pon Automotive Data Engineering Lab | Powered by Snowflake")

st.markdown("""
**Business Question:** Which region has the fastest EV growth, 
and does that correlate with charging infrastructure availability?

**Data Source:** RDW Open Data (Dutch Vehicle Authority)
""")

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 EV Growth", 
    "⛽ Fuel Distribution", 
    "🔌 Charging Infrastructure", 
    "⚡ Platform Comparison"
])

with tab1:
    st.subheader("Electric Vehicle Registrations by Year")
    
    try:
        ev_df = session.sql("""
            SELECT 
                registration_year AS year,
                fuel_category,
                COUNT(*) AS count
            FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
            WHERE registration_year >= 2015 
              AND fuel_type IS NOT NULL
            GROUP BY registration_year, fuel_category
            ORDER BY registration_year
        """).to_pandas()
        
        if not ev_df.empty:
            pivot_df = ev_df.pivot(
                index='YEAR', 
                columns='FUEL_CATEGORY', 
                values='COUNT'
            ).fillna(0)
            
            st.bar_chart(pivot_df)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                total = ev_df['COUNT'].sum()
                st.metric("Total Vehicles", f"{total:,}")
            with col2:
                ev = ev_df[ev_df['FUEL_CATEGORY'] == 'Electric']['COUNT'].sum()
                st.metric("Electric Vehicles", f"{ev:,}")
            with col3:
                hybrid = ev_df[ev_df['FUEL_CATEGORY'] == 'Hybrid']['COUNT'].sum()
                st.metric("Hybrid Vehicles", f"{hybrid:,}")
        else:
            st.info("Loading data... Dynamic Tables are refreshing.")
            
    except Exception as e:
        st.error(f"Query error: {e}")

with tab2:
    st.subheader("Fuel Type Distribution")
    
    try:
        fuel_df = session.sql("""
            SELECT 
                brandstof_omschrijving AS fuel_type,
                COUNT(*) AS count
            FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
            GROUP BY brandstof_omschrijving
            ORDER BY count DESC
        """).to_pandas()
        
        col1, col2 = st.columns([2, 1])
        with col1:
            st.bar_chart(fuel_df.set_index('FUEL_TYPE'))
        with col2:
            st.dataframe(fuel_df, hide_index=True, use_container_width=True)
            
    except Exception as e:
        st.error(f"Query error: {e}")

with tab3:
    st.subheader("Charging Infrastructure by Postal Area")
    
    try:
        charging_df = session.sql("""
            SELECT 
                postal_area,
                SUM(num_parking_locations) AS locations,
                SUM(total_charging_points) AS charging_points
            FROM PON_EV_LAB.CURATED.CHARGING_BY_AREA
            WHERE postal_area IS NOT NULL
            GROUP BY postal_area
            ORDER BY charging_points DESC NULLS LAST
            LIMIT 20
        """).to_pandas()
        
        if not charging_df.empty:
            st.bar_chart(charging_df.set_index('POSTAL_AREA')['CHARGING_POINTS'])
            
            col1, col2 = st.columns(2)
            with col1:
                total_points = charging_df['CHARGING_POINTS'].sum()
                st.metric("Total Charging Points", f"{total_points:,.0f}")
            with col2:
                num_areas = len(charging_df)
                st.metric("Postal Areas Covered", f"{num_areas}")
        else:
            st.info("Charging data loading...")
            
    except Exception as e:
        st.error(f"Query error: {e}")

with tab4:
    st.subheader("⚡ Why Snowflake?")
    
    st.markdown("""
    ### Addressing Pon's Pain Points
    
    | Challenge | Current State | Snowflake Solution |
    |-----------|--------------|-------------------|
    | **6-hour queries** | Slow processing | Instant elastic scaling |
    | **Manual scaling** | Cluster management | Zero infrastructure |
    | **Concurrency issues** | Sessions closed | Multi-cluster auto-scale |
    | **High maintenance** | Complex pipelines | Dynamic Tables |
    | **Data silos** | Copies everywhere | Secure Data Sharing |
    """)
    
    st.markdown("---")
    
    st.markdown("""
    ### Platform Comparison
    
    | Capability | Snowflake | Databricks | Microsoft Fabric |
    |------------|-----------|------------|------------------|
    | **Cluster Management** | None (serverless) | Manual | Capacity units |
    | **Startup Time** | Instant | 2-5 minutes | Variable |
    | **Concurrent Users** | Auto-scale | Manual scaling | Shared capacity |
    | **Pipeline Orchestration** | Dynamic Tables | Workflows/Airflow | Data Factory |
    | **Cost Control** | Resource Monitors | Complex DBUs | Capacity allocation |
    | **Cross-Org Sharing** | Native (zero-copy) | Delta Sharing | Not available |
    | **Embedded BI** | Streamlit native | Separate deploy | Power BI |
    """)
    
    st.markdown("---")
    
    st.subheader("Live Performance Test")
    
    if st.button("Run Performance Test", type="primary"):
        import time
        
        queries = [
            ("Count all vehicles", "SELECT COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_RAW"),
            ("Join vehicles + fuel", "SELECT COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL"),
            ("Aggregate by fuel type", "SELECT fuel_category, COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL GROUP BY 1"),
            ("Year-over-year analysis", "SELECT * FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH"),
        ]
        
        results = []
        progress = st.progress(0)
        
        for i, (name, sql) in enumerate(queries):
            start = time.time()
            try:
                session.sql(sql).collect()
                elapsed = round((time.time() - start) * 1000, 1)
                results.append({"Query": name, "Time (ms)": elapsed, "Status": "✅"})
            except Exception as e:
                results.append({"Query": name, "Time (ms)": "-", "Status": f"❌"})
            progress.progress((i + 1) / len(queries))
        
        st.dataframe(pd.DataFrame(results), hide_index=True, use_container_width=True)
        st.success("All queries completed — no cluster warmup needed!")

st.divider()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.caption("**Database:** PON_EV_LAB")
with col2:
    st.caption("**Warehouse:** PON_ANALYTICS_WH")
with col3:
    st.caption("**Monitor:** PON_LAB_MONITOR")
with col4:
    st.caption("**Share:** PON_DEALER_SHARE")
