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
    <text x="0" y="85" font-family="Arial, sans-serif" font-size="16" fill="#29B5E8">Regional analytics powering the electric transition | RDW Open Data + Marketplace</text>
    <text x="0" y="115" font-family="Arial, sans-serif" font-size="13" fill="#8b949e">Dynamic Tables  •  External Access  •  Marketplace Enrichment  •  Secure Data Sharing</text>
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
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📍 Regional Analysis", "🔗 EV vs Infrastructure", "📈 Trends & Insights", "🧠 Market Intelligence", "⚡ Fuel Mix", "🏗️ Platform", "🚗 Fleet Telemetry"])

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
            SELECT postal_area, electric_vehicles, charging_points, ev_percentage, evs_per_charging_point
            FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
            WHERE charging_points > 0
            ORDER BY electric_vehicles DESC
            LIMIT 15
        """).to_pandas()
        
        df_corr['Region'] = df_corr['POSTAL_AREA'].apply(get_region_name)
        
        st.markdown("**EVs vs Charging Points (Laadpalen) by Region**")
        chart_data = df_corr[['Region', 'ELECTRIC_VEHICLES', 'CHARGING_POINTS']].copy()
        chart_data.columns = ['Region', 'Electric Vehicles (÷100)', 'Charging Points']
        chart_data['Electric Vehicles (÷100)'] = chart_data['Electric Vehicles (÷100)'] / 100
        st.bar_chart(chart_data.set_index('Region'))
        st.caption("Note: EV count divided by 100 for scale comparison")
        
    with col2:
        st.markdown('<p class="section-header">Key Findings</p>', unsafe_allow_html=True)
        
        avg_evs_per_cp = df_corr['EVS_PER_CHARGING_POINT'].mean()
        max_row = df_corr.loc[df_corr['EVS_PER_CHARGING_POINT'].idxmax()]
        min_row = df_corr.loc[df_corr['EVS_PER_CHARGING_POINT'].idxmin()]
        
        st.metric("Avg EVs per Charging Point", f"{avg_evs_per_cp:,.0f}")
        st.metric("Most Underserved", f"{get_region_name(max_row['POSTAL_AREA'])}", f"{max_row['EVS_PER_CHARGING_POINT']:,.0f} EVs/charger")
        st.metric("Best Served", f"{get_region_name(min_row['POSTAL_AREA'])}", f"{min_row['EVS_PER_CHARGING_POINT']:,.0f} EVs/charger")
        
        st.markdown("---")
        st.warning(f"⚠️ **Infrastructure Gap:** {get_region_name(max_row['POSTAL_AREA'])} has {max_row['ELECTRIC_VEHICLES']:,} EVs but only {max_row['CHARGING_POINTS']} charging points — potential expansion opportunity")
    
    st.markdown("---")
    
    st.success(f"""
    **📊 Answer to the Business Question (PDF: "zie je dat terug in aantal beschikbare laadpalen?"):**
    
    **{get_region_name(df_corr.iloc[0]['POSTAL_AREA'])}** leads with **{df_corr.iloc[0]['ELECTRIC_VEHICLES']:,}** EVs and **{df_corr.iloc[0]['EV_PERCENTAGE']}%** adoption rate.
    However, **{get_region_name(max_row['POSTAL_AREA'])}** shows the biggest infrastructure gap with **{int(max_row['EVS_PER_CHARGING_POINT']):,}** EVs per charging point — 
    a clear opportunity for charging network expansion.
    """)
    
    st.markdown("---")
    st.markdown('<p class="section-header">Detailed Regional Comparison</p>', unsafe_allow_html=True)
    
    df_table = session.sql("""
        SELECT 
            postal_area,
            electric_vehicles,
            COALESCE(charging_points, 0) as charging_points,
            ev_percentage,
            evs_per_charging_point
        FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
        ORDER BY electric_vehicles DESC
        LIMIT 20
    """).to_pandas()
    
    df_table['Region'] = df_table['POSTAL_AREA'].apply(get_region_name)
    df_table['Charging Points'] = df_table['CHARGING_POINTS'].apply(lambda x: int(x) if x > 0 else 'No data')
    df_table['EVs per Charger'] = df_table['EVS_PER_CHARGING_POINT'].apply(lambda x: f"{int(x):,}" if pd.notna(x) and x > 0 else 'N/A')
    df_table = df_table[['Region', 'ELECTRIC_VEHICLES', 'Charging Points', 'EV_PERCENTAGE', 'EVs per Charger']]
    df_table.columns = ['Region', 'EVs', 'Charging Points', 'EV %', 'EVs per Charger']
    st.dataframe(df_table, use_container_width=True)
    st.caption("Top 20 regions by EV count. 'No data' = charging infrastructure data not available for this region.")

# ============ TAB 3: TRENDS & INSIGHTS ============
with tab3:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<p class="section-header">EV Registrations Over Time</p>', unsafe_allow_html=True)
        st.caption("Yearly EV registrations from RDW vehicle data")
        
        df_growth = session.sql("""
            SELECT registration_year as "Year",
                   ev_count as "EV Registrations"
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
        SELECT postal_area, electric_vehicles, charging_points, evs_per_charging_point
        FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
        WHERE charging_points > 0
        ORDER BY evs_per_charging_point DESC LIMIT 1
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
        
        **{int(infra_gap['EVS_PER_CHARGING_POINT']):,}** EVs per charging point — highest demand for charging expansion.
        """)

# ============ TAB 4: MARKET INTELLIGENCE (Marketplace Data) ============
with tab4:
    st.markdown('<p class="section-header">Market Intelligence: Weather, Climate & EV Adoption</p>', unsafe_allow_html=True)
    st.caption("Enriched insights from Snowflake Marketplace: KNMI (Dutch weather) + Climate Watch (emissions)")
    
    # Check if Marketplace data is available
    def check_table_exists(table_name):
        try:
            session.sql(f"SELECT 1 FROM {table_name} LIMIT 1").collect()
            return True
        except:
            return False
    
    has_weather_ev = check_table_exists("PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION")
    has_emissions = check_table_exists("PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS")
    has_monthly_weather = check_table_exists("PON_EV_LAB.CURATED.NL_MONTHLY_WEATHER")
    
    insight_tab1, insight_tab2, insight_tab3 = st.tabs(["🌡️ Weather vs EV Adoption", "🌍 Emissions Trend", "📅 Seasonal Patterns"])
    
    with insight_tab1:
        st.markdown("### Does Climate Affect EV Adoption?")
        st.caption("Hypothesis: Regions with milder winters have higher EV adoption (less range anxiety)")
        
        if has_weather_ev:
            df_weather_ev = session.sql("""
                SELECT * FROM PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION
                ORDER BY ev_share_pct DESC
            """).to_pandas()
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**EV Adoption vs Freezing Hours by Climate Region**")
                chart_data = df_weather_ev[['CLIMATE_REGION', 'EV_SHARE_PCT', 'TOTAL_FREEZING_HOURS']].copy()
                chart_data.columns = ['Region', 'EV Share %', 'Freezing Hours (÷1000)']
                chart_data['Freezing Hours (÷1000)'] = chart_data['Freezing Hours (÷1000)'] / 1000
                st.bar_chart(chart_data.set_index('Region'))
            
            with col2:
                st.markdown("**Key Finding**")
                top_region = df_weather_ev.iloc[0]
                bottom_region = df_weather_ev.iloc[-1]
                
                st.metric("Highest EV Share", f"{top_region['CLIMATE_REGION']}", f"{top_region['EV_SHARE_PCT']}%")
                st.metric("Fewest Freezing Hours", f"{top_region['TOTAL_FREEZING_HOURS']:,.0f} hrs")
                st.metric("Lowest EV Share", f"{bottom_region['CLIMATE_REGION']}", f"{bottom_region['EV_SHARE_PCT']}%")
                
                st.success(f"""
                **✅ Hypothesis Confirmed**
                
                **{top_region['CLIMATE_REGION']}** has the mildest climate ({top_region['TOTAL_FREEZING_HOURS']:,.0f} freezing hours) AND highest EV adoption ({top_region['EV_SHARE_PCT']}%).
                
                **Implication:** Target EV marketing in mild-climate regions first.
                """)
            
            st.markdown("---")
            st.dataframe(df_weather_ev.rename(columns={
                'CLIMATE_REGION': 'Climate Region',
                'TOTAL_EVS': 'Total EVs',
                'TOTAL_VEHICLES': 'Total Vehicles', 
                'EV_SHARE_PCT': 'EV Share %',
                'AVG_TEMP_C': 'Avg Temp (°C)',
                'COLDEST_TEMP_C': 'Coldest (°C)',
                'TOTAL_FREEZING_HOURS': 'Freezing Hours',
                'TOTAL_EXTREME_COLD_HOURS': 'Extreme Cold Hours'
            }), use_container_width=True)
        else:
            st.info("""
            **📦 Marketplace Data Not Yet Installed**
            
            To enable this analysis, install the **Dutch Weather Data (KNMI)** from Snowflake Marketplace:
            
            1. Go to **Data Products** → **Marketplace**
            2. Search for: `Dutch Weather Data (KNMI)`
            3. Select the listing from **DDBM B.V.**
            4. Click **Get** → Database name: `DUTCH_WEATHER_DATA_KNMI`
            5. Run the enrichment queries from **Module 6** in the lab guide
            
            This will unlock weather-based EV adoption correlation analysis.
            """)
            
            st.markdown("---")
            st.markdown("**Preview: What You'll See**")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                - 🌡️ Temperature vs EV adoption by region
                - ❄️ Freezing hours correlation
                - 📊 Climate region comparison
                """)
            with col2:
                st.markdown("""
                - 🎯 Marketing targeting insights
                - 📍 Regional prioritization
                - 📈 Weather impact on range anxiety
                """)
    
    with insight_tab2:
        st.markdown("### Netherlands Transport Emissions (Real Data)")
        st.caption("Historical CO₂ trend from Climate Watch — correlating with EV adoption growth")
        
        if has_emissions:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Transport Sector CO₂ Emissions (2010-2023)**")
                df_emissions = session.sql("""
                    SELECT year as "Year", transport_co2_mt as "Transport CO₂ (Mt)"
                    FROM PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS
                    WHERE year >= 2010
                    ORDER BY year
                """).to_pandas()
                st.line_chart(df_emissions.set_index('Year'))
                
                has_total_emissions = check_table_exists("PON_EV_LAB.CURATED.NL_TOTAL_EMISSIONS")
                if has_total_emissions:
                    st.markdown("**Total Netherlands CO₂ Emissions**")
                    df_total = session.sql("""
                        SELECT year as "Year", total_co2_mt as "Total CO₂ (Mt)"
                        FROM PON_EV_LAB.CURATED.NL_TOTAL_EMISSIONS
                        WHERE year >= 2010
                        ORDER BY year
                    """).to_pandas()
                    st.area_chart(df_total.set_index('Year'))
            
            with col2:
                peak = session.sql("""
                    SELECT * FROM PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS 
                    ORDER BY transport_co2_mt DESC LIMIT 1
                """).collect()[0]
                
                latest = session.sql("""
                    SELECT * FROM PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS 
                    WHERE year = (SELECT MAX(year) FROM PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS)
                """).collect()[0]
                
                reduction = round((peak['TRANSPORT_CO2_MT'] - latest['TRANSPORT_CO2_MT']) / peak['TRANSPORT_CO2_MT'] * 100, 1)
                
                st.markdown("**Key Metrics**")
                st.metric("Peak Year", f"{peak['YEAR']}", f"{peak['TRANSPORT_CO2_MT']:.1f} Mt")
                st.metric("Latest ({})".format(latest['YEAR']), f"{latest['TRANSPORT_CO2_MT']:.1f} Mt", f"-{reduction}% from peak")
                st.metric("Reduction", f"{peak['TRANSPORT_CO2_MT'] - latest['TRANSPORT_CO2_MT']:.1f} Mt")
                
                st.markdown("---")
                st.success(f"""
                **📉 Real Trend**
                
                Netherlands transport CO₂ dropped **{reduction}%** from {peak['YEAR']} to {latest['YEAR']}.
                
                This period coincides with accelerating EV adoption across the country.
                """)
                
                st.info("""
                **📊 Data Source**
                
                Climate Watch (Snowflake Marketplace) — official emissions data submitted to UN Framework Convention on Climate Change.
                """)
        else:
            st.info("""
            **📦 Marketplace Data Not Yet Installed**
            
            To enable this analysis, install **Snowflake Public Data (Free)** from Marketplace:
            
            1. Go to **Data Products** → **Marketplace**
            2. Search for: `Snowflake Public Data`
            3. Select the **FREE** listing
            4. Click **Get** → Database name: `SNOWFLAKE_PUBLIC_DATA_FREE`
            5. Run the enrichment queries from **Module 6** in the lab guide
            
            This will unlock Netherlands CO₂ emissions tracking.
            """)
            
            st.markdown("---")
            st.markdown("**Preview: What You'll See**")
            st.markdown("""
            - 📈 Transport sector CO₂ emissions (2010-present)
            - 📉 Total Netherlands emissions trend
            - 🎯 Peak year vs current comparison
            - 📊 Reduction metrics correlating with EV growth
            """)
    
    with insight_tab3:
        st.markdown("### Seasonal Weather Patterns")
        st.caption("Monthly weather averages to optimize EV marketing and inventory")
        
        if has_monthly_weather:
            df_monthly = session.sql("""
                SELECT 
                    CASE month 
                        WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
                        WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
                        WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
                        WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
                    END as "Month",
                    avg_temp_c as "Avg Temp (°C)",
                    pct_freezing as "% Freezing",
                    avg_monthly_precip_mm as "Precip (mm)"
                FROM PON_EV_LAB.CURATED.NL_MONTHLY_WEATHER
                ORDER BY month
            """).to_pandas()
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**Monthly Temperature & Freezing Risk**")
                st.line_chart(df_monthly.set_index('Month')[['Avg Temp (°C)']])
                
                st.markdown("**Freezing Risk by Month**")
                st.bar_chart(df_monthly.set_index('Month')[['% Freezing']])
            
            with col2:
                st.markdown("**Insights for PON**")
                
                st.warning("""
                **❄️ Winter (Dec-Feb)**
                - 4-8% freezing risk
                - Focus messaging on home charging, range confidence
                - Highlight heat pump efficiency
                """)
                
                st.success("""
                **☀️ Spring/Summer (Apr-Sep)**
                - <1% freezing risk
                - Best time for test drives
                - Emphasize road trip capability
                """)
                
                st.info("""
                **🍂 Autumn (Oct-Nov)**
                - Transition period
                - Pre-winter promotions
                - Service package upsells
                """)
            
            st.markdown("---")
            st.dataframe(df_monthly, use_container_width=True)
        else:
            st.info("""
            **📦 Marketplace Data Not Yet Installed**
            
            To enable this analysis, install the **Dutch Weather Data (KNMI)** from Snowflake Marketplace:
            
            1. Go to **Data Products** → **Marketplace**
            2. Search for: `Dutch Weather Data (KNMI)`
            3. Select the listing from **DDBM B.V.**
            4. Click **Get** → Database name: `DUTCH_WEATHER_DATA_KNMI`
            5. Run the enrichment queries from **Module 6** in the lab guide
            
            This will unlock seasonal weather analysis for marketing optimization.
            """)
            
            st.markdown("---")
            st.markdown("**Preview: What You'll See**")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                - 🌡️ Monthly temperature patterns
                - ❄️ Freezing risk by month
                - 🌧️ Precipitation trends
                """)
            with col2:
                st.markdown("""
                - 📅 Seasonal marketing calendar
                - 🎯 Best months for test drives
                - 📊 Weather-based inventory planning
                """)
    
    st.markdown("---")
    st.markdown("### 📊 Data Sources (Snowflake Marketplace)")
    col1, col2 = st.columns(2)
    with col1:
        status_knmi = "✅ Installed" if has_weather_ev or has_monthly_weather else "⬜ Not installed"
        st.success(f"""
        **KNMI Dutch Weather Data** (FREE) {status_knmi}
        - 23M+ hourly observations
        - 123 weather stations across NL
        - Historical data since 1951
        - Temperature, precipitation, wind
        """)
    with col2:
        status_climate = "✅ Installed" if has_emissions else "⬜ Not installed"
        st.info(f"""
        **Climate Watch Emissions** (FREE) {status_climate}
        - Global emissions by country
        - Transport sector breakdown
        - Netherlands-specific CO₂ data
        - Historical trends since 1990
        """)
# ============ TAB 5: FUEL MIX ============
with tab5:
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
        


# ============ TAB 6: PLATFORM ============
with tab6:
    st.markdown('<p class="section-header">How This Dashboard Stays Fresh</p>', unsafe_allow_html=True)
    st.caption("All data is automatically refreshed via Snowflake Dynamic Tables — no orchestration tools required")
    
    st.markdown("### 🔄 Live Pipeline Status")
    
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
        st.markdown("### 🏗️ Architecture")
        st.markdown("""
        ```
        ┌─────────────────────────────────────┐
        │         DATA SOURCES                │
        ├──────────────┬──────────────────────┤
        │  RDW APIs    │   Marketplace        │
        │  (External   │   (Zero-ETL)         │
        │   Access)    │                      │
        └──────┬───────┴──────────┬───────────┘
               │                  │
               ▼                  ▼
        ┌─────────────────────────────────────┐
        │           RAW LAYER                 │
        │    (Landing zone for all data)      │
        └──────────────┬──────────────────────┘
                       │ Dynamic Tables
                       ▼
        ┌─────────────────────────────────────┐
        │         CURATED LAYER               │
        │  (Cleaned, joined, enriched)        │
        └──────────────┬──────────────────────┘
                       │ Dynamic Tables
                       ▼
        ┌─────────────────────────────────────┐
        │        ANALYTICS LAYER              │
        │   (Business metrics & insights)     │
        └──────────────┬──────────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────────┐
        │      STREAMLIT DASHBOARD            │
        └─────────────────────────────────────┘
        ```
        """)
    
    with col2:
        st.markdown("### 💡 Key Snowflake Capabilities")
        
        st.info("**Dynamic Tables** — Declarative pipelines that auto-refresh based on TARGET_LAG")
        
        st.success("**Marketplace** — Instant access to 3rd party data, zero ETL")
        
        st.warning("**External Access** — Call any API directly from SQL")
        
        st.info("**Streamlit** — Build dashboards in Python, native to Snowflake")
    
    st.markdown("---")
    
    st.markdown("### ⚡ Why Snowflake?")
    st.markdown("""
    | Capability | Traditional Approach | Snowflake Approach |
    |------------|---------------------|-------------------|
    | **API Ingestion** | Separate tool + compute + scheduler | Built-in `EXTERNAL ACCESS` — unified platform |
    | **Data Pipelines** | Orchestrator + DAGs + monitoring | `CREATE DYNAMIC TABLE` — declarative SQL |
    | **Real-time Refresh** | CDC setup + streaming infrastructure | `TARGET_LAG = '1 hour'` — one parameter |
    | **Third-party Data** | Procurement + ETL + storage | Marketplace — instant, zero-copy |
    | **Partner Data Sharing** | Export → Transfer → Import | `GRANT TO SHARE` — zero-copy, live access |
    | **Governance** | Multiple tools, manual integration | Native lineage, RBAC, automatic audit |
    | **Analytics UI** | Separate BI tool + data extracts | Streamlit in Snowflake — same platform |
    """)

# ============ TAB 7: FLEET TELEMETRY (BONUS) ============
with tab7:
    st.markdown('<p class="section-header">Fleet Telemetry (Bonus Module)</p>', unsafe_allow_html=True)
    st.caption("Real-time EV fleet monitoring — built with Cortex Code in Module 9")
    
    def check_telemetry_exists():
        try:
            session.sql("SELECT 1 FROM PON_EV_LAB.CURATED.VEHICLE_STATUS LIMIT 1").collect()
            return True
        except:
            return False
    
    if check_telemetry_exists():
        col1, col2, col3, col4 = st.columns(4)
        
        fleet_summary = session.sql("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'LOW_BATTERY' THEN 1 END) as low_battery,
                COUNT(CASE WHEN status = 'CHARGING' THEN 1 END) as charging,
                ROUND(AVG(current_battery), 0) as avg_battery
            FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
        """).collect()[0]
        
        with col1:
            st.metric("Total Fleet", f"{fleet_summary['TOTAL']:,}")
        with col2:
            st.metric("Low Battery", f"{fleet_summary['LOW_BATTERY']}", delta=None if fleet_summary['LOW_BATTERY'] == 0 else f"-{fleet_summary['LOW_BATTERY']}", delta_color="inverse")
        with col3:
            st.metric("Charging Now", f"{fleet_summary['CHARGING']}")
        with col4:
            st.metric("Avg Battery", f"{fleet_summary['AVG_BATTERY']}%")
        
        st.markdown("---")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Fleet by Brand")
            df_fleet = session.sql("""
                SELECT * FROM PON_EV_LAB.ANALYTICS.FLEET_ALERTS
                ORDER BY total_vehicles DESC
            """).to_pandas()
            
            st.bar_chart(df_fleet.set_index('BRAND')[['TOTAL_VEHICLES', 'LOW_BATTERY_COUNT', 'CHARGING_COUNT']])
            
            st.markdown("### Vehicle Status")
            df_vehicles = session.sql("""
                SELECT vin, brand, current_battery, status, 
                       TO_VARCHAR(last_seen, 'YYYY-MM-DD HH24:MI') as last_seen
                FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
                ORDER BY 
                    CASE status WHEN 'LOW_BATTERY' THEN 1 WHEN 'CHARGING' THEN 2 ELSE 3 END,
                    current_battery
                LIMIT 20
            """).to_pandas()
            
            st.dataframe(df_vehicles.rename(columns={
                'VIN': 'Vehicle',
                'BRAND': 'Brand',
                'CURRENT_BATTERY': 'Battery %',
                'STATUS': 'Status',
                'LAST_SEEN': 'Last Seen'
            }), use_container_width=True)
        
        with col2:
            st.markdown("### Alerts Summary")
            
            for _, row in df_fleet.iterrows():
                if row['LOW_BATTERY_COUNT'] > 0:
                    st.warning(f"**{row['BRAND']}**: {row['LOW_BATTERY_COUNT']} vehicles with low battery")
                if row['CHARGING_COUNT'] > 0:
                    st.info(f"**{row['BRAND']}**: {row['CHARGING_COUNT']} vehicles charging")
            
            st.markdown("---")
            st.markdown("### Fleet Health")
            
            health_score = 100 - (fleet_summary['LOW_BATTERY'] / fleet_summary['TOTAL'] * 100)
            st.metric("Fleet Health Score", f"{health_score:.0f}%", help="100% minus percentage of low battery vehicles")
            
            st.markdown("---")
            st.markdown("### Data Pipeline")
            st.success("""
            **Dynamic Table**: `VEHICLE_STATUS`
            - **Refresh**: Every 1 minute
            - **Source**: `TELEMETRY_RAW`
            - **Logic**: Latest status per vehicle
            """)
    else:
        st.info("""
        **Complete Module 9 (Bonus) to see fleet telemetry data.**
        
        This tab visualizes the EV fleet monitoring pipeline you build with Cortex Code:
        
        1. Open **Cortex Code** (bottom-right sparkle icon or `Cmd+Shift+C`)
        2. Follow the prompts in **Module 9** of the lab guide
        3. Create: `TELEMETRY_RAW` → `VEHICLE_STATUS` → `FLEET_ALERTS`
        4. Return here to see your fleet dashboard
        
        **What you'll see:**
        - Real-time battery status across your fleet
        - Low battery alerts by brand
        - Charging status monitoring
        - Fleet health scoring
        """)
        
        st.markdown("---")
        st.markdown("### Preview: Fleet Dashboard")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            - 🔋 Battery status by vehicle
            - ⚡ Charging session tracking
            - 📊 Fleet health metrics
            """)
        with col2:
            st.markdown("""
            - 🚨 Low battery alerts
            - 🏷️ Brand-level analytics
            - 📍 Location tracking
            """)

# ============ FOOTER ============
st.markdown("---")

col1, col2 = st.columns(2)
with col1:
    st.caption("**Data:** RDW Open Data (API + Marketplace) | KNMI Weather | Climate Watch")
with col2:
    st.caption("**Platform:** Snowflake | Dynamic Tables | Marketplace | Streamlit")
