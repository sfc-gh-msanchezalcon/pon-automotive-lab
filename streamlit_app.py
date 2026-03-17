"""
PON EV INTELLIGENCE
Regional EV Analytics for Strategic Planning
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Pon EV Intelligence", page_icon="🚗", layout="wide")

session = get_active_session()

# Dutch postal code to region mapping
POSTAL_NAMES = {
    '10': 'Amsterdam Centrum', '11': 'Amsterdam Zuid', '12': 'Amsterdam Oost', '13': 'Amsterdam Noord',
    '14': 'Zaandam', '15': 'Purmerend', '16': 'Alkmaar', '17': 'Hoorn', '18': 'Den Helder',
    '20': 'Haarlem', '21': 'Hoofddorp', '22': 'Hillegom', '23': 'Leiden', '24': 'Alphen a/d Rijn',
    '25': 'Den Haag', '26': 'Delft', '27': 'Zoetermeer', '28': 'Gouda', '29': 'Dordrecht',
    '30': 'Rotterdam West', '31': 'Rotterdam Centrum', '32': 'Rotterdam Zuid', '33': 'Spijkenisse',
    '34': 'Hellevoetsluis', '35': 'Utrecht', '36': 'Zeist', '37': 'Amersfoort', '38': 'Hilversum',
    '39': 'Almere', '40': 'Lelystad', '41': 'Harderwijk', '42': 'Apeldoorn', '43': 'Deventer',
    '44': 'Zwolle', '45': 'Emmeloord', '46': 'Kampen', '47': 'Hengelo', '48': 'Enschede',
    '49': 'Almelo', '50': 'Eindhoven', '51': 'Helmond', '52': 'Tilburg', '53': 'Breda',
    '54': 'Bergen op Zoom', '55': 'Roosendaal', '56': 'Goes', '57': 'Middelburg', '58': 'Vlissingen',
    '60': 'Arnhem', '61': 'Nijmegen', '62': 'Venlo', '63': 'Roermond', '64': 'Maastricht',
    '65': 'Sittard', '66': 'Heerlen', '67': 'Weert', '68': 'Doetinchem', '69': 'Tiel',
    '70': 'Oss', '71': 's-Hertogenbosch', '72': 'Veghel', '73': 'Cuijk', '74': 'Venray',
    '75': 'Ede', '76': 'Barneveld', '77': 'Nijkerk', '78': 'Ermelo', '79': 'Nunspeet',
    '80': 'Zwolle Oost', '81': 'Raalte', '82': 'Ommen', '83': 'Hoogeveen', '84': 'Emmen',
    '85': 'Assen', '86': 'Meppel', '87': 'Steenwijk', '88': 'Leeuwarden', '89': 'Sneek',
    '90': 'Drachten', '91': 'Heerenveen', '92': 'Dokkum', '93': 'Groningen', '94': 'Veendam',
    '95': 'Winschoten', '96': 'Stadskanaal', '97': 'Hoogezand', '98': 'Delfzijl', '99': 'Appingedam'
}

def get_region_name(postal_area):
    return POSTAL_NAMES.get(str(postal_area), f"Area {postal_area}")

st.markdown("""
<style>
    .section-header { font-size: 1.1rem; font-weight: 600; color: #1a365d; margin-bottom: 0.5rem; }
    .stMetric label { color: #1a365d !important; }
</style>
<svg width="100%" height="140" viewBox="0 0 1100 140" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet" style="margin-bottom: 1.5rem; border-radius: 8px;">
  <defs>
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#0d1117;stop-opacity:1" />
      <stop offset="100%" style="stop-color:#161b22;stop-opacity:1" />
    </linearGradient>
  </defs>
  <rect width="100%" height="100%" fill="url(#bgGrad)" rx="8"/>
  <rect x="0" y="0" width="100%" height="3" fill="#29B5E8" rx="8"/>
  <g transform="translate(40, 35)">
    <path d="M0,12 Q0,0 12,0 L28,0 Q40,0 40,12 L40,28 Q40,40 28,40 L12,40 L12,60 L0,60 L0,12 Z M12,10 L12,30 L28,30 Q30,30 30,28 L30,12 Q30,10 28,10 Z" fill="white"/>
    <path d="M55,12 Q55,0 67,0 L83,0 Q95,0 95,12 L95,28 Q95,40 83,40 L67,40 Q55,40 55,28 Z M67,10 L67,30 L83,30 Q85,30 85,28 L85,12 Q85,10 83,10 Z" fill="white"/>
    <path d="M110,40 L110,12 Q110,0 122,0 L138,0 Q150,0 150,12 L150,40 L138,40 L138,12 Q138,10 136,10 L124,10 Q122,10 122,12 L122,40 Z" fill="white"/>
    <rect x="-5" y="68" width="160" height="4" fill="white"/>
  </g>
  <rect x="220" y="30" width="2" height="80" fill="#29B5E8" opacity="0.4" rx="1"/>
  <g transform="translate(250, 0)">
    <text x="0" y="55" font-family="Arial, sans-serif" font-size="28" font-weight="bold" fill="white">EV Intelligence</text>
    <text x="0" y="85" font-family="Arial, sans-serif" font-size="16" fill="#29B5E8">Regional analytics powering the electric transition | RDW Open Data</text>
    <text x="0" y="115" font-family="Arial, sans-serif" font-size="13" fill="#8b949e">Dynamic Tables  •  External Access  •  Secure Data Sharing</text>
  </g>
  <g transform="translate(950, 45)">
    <rect x="0" y="30" width="60" height="20" rx="6" fill="#29B5E8"/>
    <rect x="8" y="22" width="42" height="12" rx="3" fill="#29B5E8"/>
    <rect x="14" y="24" width="12" height="7" rx="1" fill="#0d1117" opacity="0.6"/>
    <rect x="32" y="24" width="12" height="7" rx="1" fill="#0d1117" opacity="0.6"/>
    <circle cx="14" cy="50" r="7" fill="#0d1117"/>
    <circle cx="14" cy="50" r="4" fill="#FF6B00"/>
    <circle cx="46" cy="50" r="7" fill="#0d1117"/>
    <circle cx="46" cy="50" r="4" fill="#FF6B00"/>
    <polygon points="58,20 54,28 57,28 53,38 60,26 57,26" fill="#00ff88"/>
  </g>
  <g transform="translate(1050, 70)">
    <line x1="0" y1="-14" x2="0" y2="14" stroke="#29B5E8" stroke-width="2"/>
    <line x1="-12" y1="-7" x2="12" y2="7" stroke="#29B5E8" stroke-width="2"/>
    <line x1="-12" y1="7" x2="12" y2="-7" stroke="#29B5E8" stroke-width="2"/>
    <circle cx="0" cy="-14" r="2.5" fill="#29B5E8"/>
    <circle cx="0" cy="14" r="2.5" fill="#29B5E8"/>
    <circle cx="-12" cy="-7" r="2.5" fill="#29B5E8"/>
    <circle cx="12" cy="7" r="2.5" fill="#29B5E8"/>
    <circle cx="-12" cy="7" r="2.5" fill="#29B5E8"/>
    <circle cx="12" cy="-7" r="2.5" fill="#29B5E8"/>
  </g>
</svg>
""", unsafe_allow_html=True)

# ============ KEY METRICS ============
summary = session.sql("""
    SELECT 
        SUM(electric_vehicles) as total_evs,
        SUM(total_vehicles) as total_vehicles,
        ROUND(100.0 * SUM(electric_vehicles) / SUM(total_vehicles), 1) as ev_share,
        COUNT(DISTINCT postal_area) as regions,
        MAX(ev_percentage) as top_region_pct
    FROM PON_EV_LAB.CURATED.EV_BY_REGION
""").collect()[0]

charging = session.sql("SELECT SUM(total_charging_points) as points FROM PON_EV_LAB.CURATED.CHARGING_BY_AREA").collect()[0][0] or 0

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total EVs", f"{summary['TOTAL_EVS']:,}", help="Battery Electric Vehicles registered in NL")
with col2:
    st.metric("National EV Share", f"{summary['EV_SHARE']}%", help="EVs as percentage of all passenger cars")
with col3:
    st.metric("Top Region", f"{summary['TOP_REGION_PCT']}%", help="Highest regional EV adoption rate")
with col4:
    st.metric("Charging Points", f"{charging:,}", help="Public charging infrastructure from RDW")
with col5:
    st.metric("Regions", f"{summary['REGIONS']}", help="Dutch postal code areas (2-digit)")

st.markdown("---")

# ============ TABS ============
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📍 Regional Analysis", "🔗 EV vs Infrastructure", "📈 Trends & Insights", "⚡ Fuel Mix", "🏗️ Platform"])

# ============ TAB 1: REGIONAL ANALYSIS ============
with tab1:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<p class="section-header">EV Adoption Rate by Region</p>', unsafe_allow_html=True)
        st.caption("Percentage of passenger cars that are fully electric")
        
        df_region = session.sql("""
            SELECT postal_area, ev_percentage, electric_vehicles, total_vehicles
            FROM PON_EV_LAB.CURATED.EV_BY_REGION
            WHERE total_vehicles > 10000
            ORDER BY ev_percentage DESC
            LIMIT 15
        """).to_pandas()
        
        df_region['Region'] = df_region['POSTAL_AREA'].apply(get_region_name)
        st.bar_chart(df_region.set_index('Region')['EV_PERCENTAGE'])
    
    with col2:
        st.markdown('<p class="section-header">Top 5 EV Regions</p>', unsafe_allow_html=True)
        
        top5 = session.sql("""
            SELECT postal_area, ev_percentage, electric_vehicles, total_vehicles
            FROM PON_EV_LAB.CURATED.EV_BY_REGION
            WHERE total_vehicles > 10000
            ORDER BY ev_percentage DESC LIMIT 5
        """).to_pandas()
        
        for i, row in top5.iterrows():
            region_name = get_region_name(row['POSTAL_AREA'])
            st.markdown(f"**#{i+1} {region_name}**")
            st.markdown(f"  {row['EV_PERCENTAGE']}% EV share")
            st.caption(f"  {row['ELECTRIC_VEHICLES']:,} EVs of {row['TOTAL_VEHICLES']:,} vehicles")
        
        st.markdown("---")
        st.info(f"🎯 **{get_region_name(top5.iloc[0]['POSTAL_AREA'])}** leads with {top5.iloc[0]['EV_PERCENTAGE']}% — nearly **4x** the national average")

# ============ TAB 2: EV VS INFRASTRUCTURE CORRELATION ============
with tab2:
    st.markdown('<p class="section-header">Does Charging Infrastructure Correlate with EV Adoption?</p>', unsafe_allow_html=True)
    st.caption("Answering Pon's key question: Which regions lead in EV growth, and how does charging infrastructure compare?")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        df_corr = session.sql("""
            SELECT postal_area, electric_vehicles, parking_locations, ev_percentage, evs_per_parking_location
            FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
            WHERE parking_locations > 0
            ORDER BY electric_vehicles DESC
            LIMIT 15
        """).to_pandas()
        
        df_corr['Region'] = df_corr['POSTAL_AREA'].apply(get_region_name)
        
        st.markdown("**EVs vs Parking Locations by Region**")
        chart_data = df_corr[['Region', 'ELECTRIC_VEHICLES', 'PARKING_LOCATIONS']].copy()
        chart_data.columns = ['Region', 'Electric Vehicles (÷100)', 'Parking Locations']
        chart_data['Electric Vehicles (÷100)'] = chart_data['Electric Vehicles (÷100)'] / 100
        st.bar_chart(chart_data.set_index('Region'))
        st.caption("Note: EV count divided by 100 for scale comparison")
        
    with col2:
        st.markdown('<p class="section-header">Key Findings</p>', unsafe_allow_html=True)
        
        avg_evs_per_loc = df_corr['EVS_PER_PARKING_LOCATION'].mean()
        max_row = df_corr.loc[df_corr['EVS_PER_PARKING_LOCATION'].idxmax()]
        min_row = df_corr.loc[df_corr['EVS_PER_PARKING_LOCATION'].idxmin()]
        
        st.metric("Avg EVs per Location", f"{avg_evs_per_loc:,.0f}")
        st.metric("Most Underserved", f"{get_region_name(max_row['POSTAL_AREA'])}", f"{max_row['EVS_PER_PARKING_LOCATION']:,.0f} EVs/location")
        st.metric("Best Served", f"{get_region_name(min_row['POSTAL_AREA'])}", f"{min_row['EVS_PER_PARKING_LOCATION']:,.0f} EVs/location")
        
        st.markdown("---")
        st.warning(f"⚠️ **Infrastructure Gap:** {get_region_name(max_row['POSTAL_AREA'])} has {max_row['ELECTRIC_VEHICLES']:,} EVs but only {max_row['PARKING_LOCATIONS']} parking locations — potential expansion opportunity")
    
    st.markdown("---")
    
    st.success(f"""
    **📊 Answer to the Business Question:**
    
    **{get_region_name(df_corr.iloc[0]['POSTAL_AREA'])}** leads with **{df_corr.iloc[0]['ELECTRIC_VEHICLES']:,}** EVs and **{df_corr.iloc[0]['EV_PERCENTAGE']}%** adoption rate.
    However, **{get_region_name(max_row['POSTAL_AREA'])}** shows the biggest infrastructure gap with **{int(max_row['EVS_PER_PARKING_LOCATION']):,}** EVs per parking location — 
    a clear opportunity for charging network expansion.
    """)
    
    st.markdown("---")
    st.markdown('<p class="section-header">Detailed Regional Comparison</p>', unsafe_allow_html=True)
    
    df_table = session.sql("""
        SELECT 
            postal_area,
            electric_vehicles,
            parking_locations,
            ev_percentage,
            evs_per_parking_location
        FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
        WHERE parking_locations > 0
        ORDER BY evs_per_parking_location DESC
        LIMIT 10
    """).to_pandas()
    
    df_table['Region'] = df_table['POSTAL_AREA'].apply(get_region_name)
    df_table = df_table[['Region', 'ELECTRIC_VEHICLES', 'PARKING_LOCATIONS', 'EV_PERCENTAGE', 'EVS_PER_PARKING_LOCATION']]
    df_table.columns = ['Region', 'EVs', 'Parking Locations', 'EV %', 'EVs per Location']
    st.dataframe(df_table, use_container_width=True)
    st.caption("Sorted by EVs per Location (higher = more infrastructure needed)")

# ============ TAB 3: TRENDS & INSIGHTS ============
with tab3:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<p class="section-header">EV Registrations Over Time</p>', unsafe_allow_html=True)
        st.caption("Annual EV registrations from vehicle sample data")
        
        df_growth = session.sql("""
            SELECT registration_year as "Year",
                   ev_count as "EV Registrations",
                   ev_share_percent as "EV Share %"
            FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH
            WHERE registration_year >= 2015
            ORDER BY registration_year
        """).to_pandas()
        
        st.bar_chart(df_growth.set_index('Year')['EV Registrations'])
    
    with col2:
        st.markdown('<p class="section-header">Year-over-Year Growth</p>', unsafe_allow_html=True)
        
        df_yoy = session.sql("""
            SELECT registration_year, ev_count, yoy_growth_percent, ev_share_percent
            FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH
            WHERE registration_year >= 2020
            ORDER BY registration_year DESC
        """).to_pandas()
        
        for _, row in df_yoy.iterrows():
            growth = row['YOY_GROWTH_PERCENT']
            delta = f"{'+' if growth and growth > 0 else ''}{growth}%" if growth else "—"
            st.metric(f"{int(row['REGISTRATION_YEAR'])}", f"{int(row['EV_COUNT']):,} EVs", delta)
    
    st.markdown("---")
    
    st.markdown('<p class="section-header">Key Insights (from data)</p>', unsafe_allow_html=True)
    
    # Get peak growth year
    peak_growth = session.sql("""
        SELECT registration_year, yoy_growth_percent 
        FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH 
        WHERE yoy_growth_percent IS NOT NULL
        ORDER BY yoy_growth_percent DESC LIMIT 1
    """).collect()[0]
    
    # Get top region
    top_region = session.sql("""
        SELECT postal_area, ev_percentage, electric_vehicles
        FROM PON_EV_LAB.CURATED.EV_BY_REGION
        ORDER BY ev_percentage DESC LIMIT 1
    """).collect()[0]
    
    # Get infrastructure gap
    infra_gap = session.sql("""
        SELECT postal_area, electric_vehicles, parking_locations, evs_per_parking_location
        FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
        WHERE parking_locations > 0
        ORDER BY evs_per_parking_location DESC LIMIT 1
    """).collect()[0]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.success(f"""
        **🚀 Peak Growth: {int(peak_growth['REGISTRATION_YEAR'])}**
        
        EV registrations grew **{peak_growth['YOY_GROWTH_PERCENT']}%** year-over-year — the largest spike on record.
        """)
    
    with col2:
        region_name = get_region_name(top_region['POSTAL_AREA'])
        st.info(f"""
        **📍 Leading Region: {region_name}**
        
        **{top_region['EV_PERCENTAGE']}%** EV adoption with {top_region['ELECTRIC_VEHICLES']:,} electric vehicles registered.
        """)
    
    with col3:
        gap_region = get_region_name(infra_gap['POSTAL_AREA'])
        st.warning(f"""
        **⚠️ Infrastructure Gap: {gap_region}**
        
        **{int(infra_gap['EVS_PER_PARKING_LOCATION']):,}** EVs per parking location — highest demand for charging expansion.
        """)

# ============ TAB 4: FUEL MIX ============
with tab4:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<p class="section-header">Fuel Distribution by Region</p>', unsafe_allow_html=True)
        st.caption("Electric vs Petrol vs Diesel by region")
        
        df_fuel = session.sql("""
            SELECT postal_area,
                   electric_vehicles as "Electric",
                   petrol_vehicles as "Petrol", 
                   diesel_vehicles as "Diesel"
            FROM PON_EV_LAB.CURATED.EV_BY_REGION
            WHERE total_vehicles > 50000
            ORDER BY electric_vehicles DESC
            LIMIT 12
        """).to_pandas()
        
        df_fuel['Region'] = df_fuel['POSTAL_AREA'].apply(get_region_name)
        st.bar_chart(df_fuel.set_index('Region')[['Electric', 'Petrol', 'Diesel']])
    
    with col2:
        st.markdown('<p class="section-header">National Totals</p>', unsafe_allow_html=True)
        
        fuel_totals = session.sql("""
            SELECT 
                SUM(electric_vehicles) as electric,
                SUM(petrol_vehicles) as petrol,
                SUM(diesel_vehicles) as diesel,
                SUM(total_vehicles) as total
            FROM PON_EV_LAB.CURATED.EV_BY_REGION
        """).collect()[0]
        
        st.metric("Electric (E)", f"{fuel_totals['ELECTRIC']:,}", f"{round(100*fuel_totals['ELECTRIC']/fuel_totals['TOTAL'],1)}%")
        st.metric("Petrol (B)", f"{fuel_totals['PETROL']:,}", f"{round(100*fuel_totals['PETROL']/fuel_totals['TOTAL'],1)}%")
        st.metric("Diesel (D)", f"{fuel_totals['DIESEL']:,}", f"{round(100*fuel_totals['DIESEL']/fuel_totals['TOTAL'],1)}%")
        


# ============ TAB 5: PLATFORM ============
with tab5:
    st.markdown('<p class="section-header">How This Dashboard Stays Fresh</p>', unsafe_allow_html=True)
    st.caption("All data is automatically refreshed via Snowflake Dynamic Tables — no orchestration tools required")
    
    # Show actual Dynamic Table status
    st.markdown("### 🔄 Live Pipeline Status")
    
    df_dt = session.sql("""
        SELECT 
            name as "Dynamic Table",
            schema_name as "Layer",
            target_lag as "Refresh Interval",
            scheduling_state as "Status",
            refresh_mode as "Mode",
            TO_VARCHAR(data_timestamp, 'YYYY-MM-DD HH24:MI') as "Last Refresh"
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-1)))
    """)
    
    # Use SHOW command and display results
    session.sql("SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB").collect()
    df_dt = session.sql("""
        SELECT 
            "name" as "Dynamic Table",
            "schema_name" as "Layer",
            "target_lag" as "Refresh Interval",
            "scheduling_state" as "Status",
            "refresh_mode" as "Mode",
            TO_VARCHAR("data_timestamp", 'YYYY-MM-DD HH24:MI') as "Last Refresh"
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        ORDER BY "schema_name", "name"
    """).to_pandas()
    
    st.dataframe(df_dt, use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏗️ How It Works")
        st.markdown("""
        ```
        ┌─────────────────┐
        │   RDW APIs      │  ← External Data Source
        │  (Live Data)    │
        └────────┬────────┘
                 │ External Access Integration
                 ▼
        ┌─────────────────┐
        │   RAW Tables    │  ← Landing Zone
        │ (API responses) │
        └────────┬────────┘
                 │ Dynamic Table (auto-refresh)
                 ▼
        ┌─────────────────┐
        │ CURATED Tables  │  ← Transformed Data
        │ (EV_BY_REGION)  │
        └────────┬────────┘
                 │ Dynamic Table (auto-refresh)
                 ▼
        ┌─────────────────┐
        │ ANALYTICS       │  ← Business Metrics
        │ (Insights)      │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │  This Dashboard │  ← Always Current
        └─────────────────┘
        ```
        """)
    
    with col2:
        st.markdown("### 💡 Key Concepts")
        
        st.info("**Dynamic Tables** — Declarative pipelines that auto-refresh. No external scheduler needed.")
        
        st.success("**External Access** — Call APIs directly from SQL. No middleware needed.")
        
        st.warning("**Data Sharing** — Share live data with partners. Zero copies, instant access.")
    
    st.markdown("---")
    
    st.markdown("### ⚡ Why Snowflake?")
    st.markdown("""
    | Capability | Traditional Approach | Snowflake Approach |
    |------------|---------------------|-------------------|
    | **API Ingestion** | Separate tool + compute + scheduler | Built-in `EXTERNAL ACCESS` — unified platform |
    | **Data Pipelines** | Orchestrator + DAGs + monitoring | `CREATE DYNAMIC TABLE` — declarative SQL |
    | **Real-time Refresh** | CDC setup + streaming infrastructure | `TARGET_LAG = '1 hour'` — one parameter |
    | **Partner Data Sharing** | Export → Transfer → Import | `GRANT TO SHARE` — zero-copy, live access |
    | **Governance** | Multiple tools, manual integration | Native lineage, RBAC, automatic audit |
    | **Scaling** | Capacity planning, manual tuning | Automatic — instant scale, per-second billing |
    | **Analytics UI** | Separate BI tool + data extracts | Streamlit in Snowflake — same platform |
    """)

# ============ FOOTER ============
st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    st.caption("**Data:** RDW Open Data (Dutch Vehicle Authority)")
with col2:
    st.caption("**Platform:** Snowflake | Dynamic Tables | Streamlit")
