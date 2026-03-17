"""
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Streamlit Dashboard for Hands-on Lab

This dashboard demonstrates Snowflake's native Streamlit integration.
No external hosting, no separate deployment - just SQL and Python.
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Pon EV Transition", page_icon="🚗", layout="wide")

session = get_active_session()

st.title("🚗 EV Transition Netherlands")
st.caption("Pon Automotive Data Engineering Lab | Powered by Snowflake")

st.markdown("""
**Business Question:** Which region has the fastest EV growth, 
and does that correlate with charging infrastructure?
""")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 EV Growth", "⛽ Fuel Mix", "🔌 Charging Infra", "🌍 Market Insights", "⚡ Performance"])

with tab1:
    st.subheader("EV Registrations by Year")
    
    df = session.sql("""
        SELECT registration_year, fuel_category, COUNT(*) as count
        FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
        WHERE registration_year >= 2015 AND fuel_type IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1
    """).to_pandas()
    
    if not df.empty:
        pivot = df.pivot(index='REGISTRATION_YEAR', columns='FUEL_CATEGORY', values='COUNT').fillna(0)
        st.bar_chart(pivot)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Vehicles", f"{df['COUNT'].sum():,}")
        with col2:
            ev = df[df['FUEL_CATEGORY'] == 'Electric']['COUNT'].sum()
            st.metric("Electric Vehicles", f"{ev:,}")

with tab2:
    st.subheader("Fuel Type Distribution")
    
    df = session.sql("""
        SELECT brandstof_omschrijving as fuel_type, COUNT(*) as count
        FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
        GROUP BY 1
        ORDER BY 2 DESC
    """).to_pandas()
    
    st.bar_chart(df.set_index('FUEL_TYPE'))

with tab3:
    st.subheader("Charging Infrastructure by Region")
    
    df = session.sql("""
        SELECT postal_area, SUM(total_charging_points) as charging_points
        FROM PON_EV_LAB.CURATED.CHARGING_BY_AREA
        WHERE postal_area IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """).to_pandas()
    
    if not df.empty:
        st.bar_chart(df.set_index('POSTAL_AREA'))

with tab4:
    st.subheader("🌍 Marketplace Data Insights")
    st.caption("Data enriched from Snowflake Marketplace - zero ETL, instant access")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("##### 🌡️ Weather Impact on EV Range")
        st.markdown("*Cold weather reduces EV range by 20-40%*")
        
        try:
            weather_df = session.sql("""
                SELECT 
                    DATE_TRUNC('month', date_valid_std) as month,
                    ROUND(AVG(avg_temperature_air_2m_f), 1) as avg_temp_f,
                    ROUND((AVG(avg_temperature_air_2m_f) - 32) * 5/9, 1) as avg_temp_c
                FROM WEATHER_SOURCE.STANDARD_TILE.HISTORY_DAY
                WHERE country = 'NL'
                    AND date_valid_std >= '2023-01-01'
                GROUP BY 1
                ORDER BY 1
            """).to_pandas()
            
            if not weather_df.empty:
                st.line_chart(weather_df.set_index('MONTH')['AVG_TEMP_C'])
                
                cold_months = weather_df[weather_df['AVG_TEMP_C'] < 10].shape[0]
                st.metric("Cold Months (<10°C)", f"{cold_months} months", 
                         delta="Range reduction expected", delta_color="inverse")
        except:
            st.info("💡 Get weather data: Marketplace → 'Global Weather & Climate Data for BI'")
    
    with col2:
        st.markdown("##### ⚡ EU Energy Prices")
        st.markdown("*Electricity costs impact EV ownership economics*")
        
        try:
            energy_df = session.sql("""
                SELECT 
                    DATE_TRUNC('quarter', date) as quarter,
                    ROUND(AVG(value), 2) as avg_price
                FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.OECD_TIMESERIES
                WHERE variable_name ILIKE '%electricity%price%'
                    AND geo ILIKE '%Netherlands%'
                    AND date >= '2020-01-01'
                GROUP BY 1
                ORDER BY 1
            """).to_pandas()
            
            if not energy_df.empty and len(energy_df) > 1:
                st.line_chart(energy_df.set_index('QUARTER'))
        except:
            st.info("💡 Get economic data: Marketplace → 'Snowflake Public Data (Free)'")
    
    st.divider()
    
    st.markdown("##### 🇳🇱 Netherlands EV Market Context")
    
    col3, col4, col5 = st.columns(3)
    
    with col3:
        try:
            emissions_df = session.sql("""
                SELECT ROUND(value, 1) as co2_per_capita
                FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.CLIMATE_WATCH_TIMESERIES
                WHERE variable_name ILIKE '%CO2%capita%'
                    AND geo ILIKE '%Netherlands%'
                ORDER BY date DESC
                LIMIT 1
            """).to_pandas()
            
            if not emissions_df.empty:
                st.metric("CO₂ per Capita", f"{emissions_df['CO2_PER_CAPITA'].iloc[0]} tons")
        except:
            st.metric("CO₂ per Capita", "8.1 tons", help="Marketplace data unavailable")
    
    with col4:
        ev_share = session.sql("""
            SELECT ROUND(100.0 * SUM(CASE WHEN fuel_category = 'Electric' THEN 1 ELSE 0 END) / COUNT(*), 1)
            FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
            WHERE registration_year = 2023
        """).collect()[0][0] or 0
        st.metric("EV Share (2023)", f"{ev_share}%", delta="+Growing")
    
    with col5:
        try:
            gdp_df = session.sql("""
                SELECT ROUND(value/1e9, 0) as gdp_billions
                FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.OECD_TIMESERIES
                WHERE variable_name ILIKE '%GDP%current prices%'
                    AND geo ILIKE '%Netherlands%'
                    AND unit ILIKE '%USD%'
                ORDER BY date DESC
                LIMIT 1
            """).to_pandas()
            
            if not gdp_df.empty:
                st.metric("GDP", f"${gdp_df['GDP_BILLIONS'].iloc[0]:.0f}B")
        except:
            st.metric("GDP", "$1.01T", help="Marketplace data unavailable")
    
    st.success("✅ All insights powered by Marketplace data - no ETL pipelines needed!")

with tab5:
    st.subheader("⚡ Why Snowflake?")
    
    st.markdown("""
    | Feature | Snowflake | Databricks | Fabric |
    |---------|-----------|------------|--------|
    | **Cluster Management** | None | Manual | Capacity units |
    | **Startup Time** | Instant | 2-5 min | Variable |
    | **Concurrent Users** | Auto-scale | Manual | Shared |
    | **Data Sharing** | Native | Delta Sharing | N/A |
    """)
    
    if st.button("Run Performance Test", type="primary"):
        import time
        results = []
        
        for name, sql in [
            ("Count vehicles", "SELECT COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_RAW"),
            ("Join tables", "SELECT COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL"),
            ("Aggregate data", "SELECT fuel_category, COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL GROUP BY 1"),
        ]:
            start = time.time()
            session.sql(sql).collect()
            ms = round((time.time() - start) * 1000, 1)
            results.append({"Query": name, "Time (ms)": ms})
        
        st.dataframe(pd.DataFrame(results), hide_index=True)
        st.success("No cluster warmup needed!")

st.divider()
st.caption("Database: PON_EV_LAB | Warehouse: PON_ANALYTICS_WH | Share: PON_DEALER_SHARE")
