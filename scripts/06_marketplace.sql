/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 6: Marketplace Data Enrichment
=============================================================================
PREREQUISITE: Get these FREE datasets from Snowflake Marketplace first:

1. "Dutch Weather Data (KNMI)" from DDBM B.V.
   → Database name: DUTCH_WEATHER_DATA_KNMI
   → Contains: 23M+ hourly weather observations from Dutch stations

2. "Snowflake Public Data (Free)" from Snowflake
   → Database name: SNOWFLAKE_PUBLIC_DATA_FREE
   → Contains: Climate Watch emissions data, economic indicators

To get datasets:
  1. Navigate to Data Products > Marketplace
  2. Search for the dataset name
  3. Click "Get" and accept terms
  4. Use the EXACT database names shown above
=============================================================================
*/

USE DATABASE PON_EV_LAB;
USE WAREHOUSE PON_ANALYTICS_WH;

-- ============================================
-- STEP 1: Verify Marketplace Databases Exist
-- ============================================

-- Check KNMI weather data is available
SELECT COUNT(*) as observation_count 
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2020;

-- Check Climate Watch data is available
SELECT COUNT(*) as emissions_records
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD';

-- ============================================
-- STEP 2: Explore KNMI Weather Data
-- ============================================

-- Sample weather observations
SELECT 
    TIME,
    STATIONNAME,
    TEMP as temperature_celsius,
    WIND_SPEED as wind_m_per_s,
    PRECIP_HOUR as precipitation_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE TIME >= '2024-01-01'
LIMIT 10;

-- Weather stations in the Netherlands
SELECT DISTINCT STATIONNAME
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
ORDER BY STATIONNAME
LIMIT 20;

-- ============================================
-- STEP 3: Weather Impact on EV Range
-- ============================================
-- Cold weather reduces EV battery range by 20-40%

-- Average temperature and freezing hours by year
SELECT 
    YEAR(TIME) as year,
    ROUND(AVG(TEMP), 1) as avg_temp_celsius,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours,
    COUNT(*) as total_observations
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2015
GROUP BY YEAR(TIME)
ORDER BY year;

-- Regional weather differences (for EV range planning)
SELECT 
    STATIONNAME,
    ROUND(AVG(TEMP), 1) as avg_temp,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2020
GROUP BY STATIONNAME
ORDER BY freezing_hours DESC
LIMIT 10;

-- ============================================
-- STEP 4: Climate Watch Emissions Data
-- ============================================

-- Netherlands transport emissions over time
SELECT 
    YEAR(DATE) as year,
    VARIABLE_NAME,
    VALUE as emissions_tonnes,
    UNIT
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'transportation_co2_climate_watch'
ORDER BY year DESC
LIMIT 20;

-- ============================================
-- STEP 5: Create Enriched Analytics Views
-- ============================================

-- Monthly weather patterns for seasonal marketing
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_MONTHLY_WEATHER AS
SELECT 
    MONTH(TIME) as month,
    ROUND(AVG(TEMP), 1) as avg_temp_c,
    ROUND(COUNT(CASE WHEN TEMP < 0 THEN 1 END) * 100.0 / COUNT(*), 1) as pct_freezing,
    ROUND(AVG(COALESCE(PRECIP_HOUR, 0)) * 30, 0) as avg_monthly_precip_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2020
GROUP BY MONTH(TIME);

-- Transport emissions view
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS AS
SELECT 
    YEAR(DATE) AS year,
    ROUND(VALUE / 1000000, 1) AS transport_co2_mt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES 
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'transportation_co2_climate_watch'
  AND YEAR(DATE) >= 2010
ORDER BY year;

-- Total emissions view for ESG reporting
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_TOTAL_EMISSIONS AS
SELECT 
    YEAR(DATE) AS year,
    ROUND(VALUE / 1000000, 1) AS total_co2_mt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES 
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'total_excluding_lulucf_co2_climate_watch'
  AND YEAR(DATE) >= 2010
ORDER BY year;

-- Regional weather vs EV adoption correlation
-- Maps postal codes to Dutch regions (10-39=Randstad, 50-66=Zuid, 88-99=Noord)
CREATE OR REPLACE VIEW PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION AS
WITH weather_by_region AS (
    SELECT 
        CASE 
            WHEN STATIONNAME LIKE '%SCHIPHOL%' OR STATIONNAME LIKE '%AMSTERDAM%' 
                 OR STATIONNAME LIKE '%ROTTERDAM%' OR STATIONNAME LIKE '%DEN HAAG%'
                 OR STATIONNAME LIKE '%UTRECHT%' OR STATIONNAME LIKE '%HAARLEM%'
                 OR STATIONNAME LIKE '%VALKENBURG%' OR STATIONNAME LIKE '%VOORSCHOTEN%' THEN 'Randstad'
            WHEN STATIONNAME LIKE '%GRONINGEN%' OR STATIONNAME LIKE '%LEEUWARDEN%' 
                 OR STATIONNAME LIKE '%EELDE%' OR STATIONNAME LIKE '%HOOGEVEEN%' THEN 'Noord'
            WHEN STATIONNAME LIKE '%MAASTRICHT%' OR STATIONNAME LIKE '%EINDHOVEN%' 
                 OR STATIONNAME LIKE '%VOLKEL%' OR STATIONNAME LIKE '%ARCEN%' THEN 'Zuid'
            ELSE 'Overig'
        END AS climate_region,
        AVG(TEMP) AS avg_temp_c,
        MIN(TEMP) AS coldest_temp_c,
        SUM(CASE WHEN TEMP < 0 THEN 1 ELSE 0 END) AS freezing_hours,
        SUM(CASE WHEN TEMP < -5 THEN 1 ELSE 0 END) AS extreme_cold_hours
    FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
    WHERE TEMP IS NOT NULL
    GROUP BY 1
),
ev_by_region AS (
    SELECT 
        CASE 
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 10 AND 39 THEN 'Randstad'
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 88 AND 99 THEN 'Noord'
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 50 AND 66 THEN 'Zuid'
            ELSE 'Overig'
        END AS region,
        SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) AS total_evs,
        SUM(aantal) AS total_vehicles
    FROM PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
    WHERE voertuigsoort = 'Personenauto'
    GROUP BY 1
)
SELECT 
    w.climate_region,
    COALESCE(e.total_evs, 0) AS total_evs,
    COALESCE(e.total_vehicles, 0) AS total_vehicles,
    ROUND(100.0 * COALESCE(e.total_evs, 0) / NULLIF(e.total_vehicles, 0), 1) AS ev_share_pct,
    ROUND(w.avg_temp_c, 1) AS avg_temp_c,
    ROUND(w.coldest_temp_c, 1) AS coldest_temp_c,
    w.freezing_hours AS total_freezing_hours,
    w.extreme_cold_hours AS total_extreme_cold_hours
FROM weather_by_region w
LEFT JOIN ev_by_region e ON w.climate_region = e.region
ORDER BY ev_share_pct DESC;

-- Yearly weather summary for correlation analysis
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_WEATHER_YEARLY AS
SELECT 
    YEAR(TIME) as year,
    ROUND(AVG(TEMP), 1) as avg_temp_celsius,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours,
    ROUND(SUM(COALESCE(PRECIP_HOUR, 0)), 0) as total_precip_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
GROUP BY YEAR(TIME);

-- Combined EV + weather + emissions view
CREATE OR REPLACE VIEW PON_EV_LAB.ANALYTICS.EV_WEATHER_EMISSIONS AS
SELECT 
    e.registration_year as year,
    e.ev_count as total_evs,
    e.yoy_growth_percent,
    w.avg_temp_celsius,
    w.freezing_hours,
    t.transport_co2_mt
FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH e
LEFT JOIN PON_EV_LAB.CURATED.NL_WEATHER_YEARLY w ON e.registration_year = w.year
LEFT JOIN PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS t ON e.registration_year = t.year
WHERE e.registration_year >= 2015
ORDER BY e.registration_year;

-- ============================================
-- STEP 6: Verify Views Created
-- ============================================

SELECT 'NL_WEATHER_YEARLY' as view_name, COUNT(*) as row_count FROM PON_EV_LAB.CURATED.NL_WEATHER_YEARLY
UNION ALL SELECT 'NL_MONTHLY_WEATHER', COUNT(*) FROM PON_EV_LAB.CURATED.NL_MONTHLY_WEATHER
UNION ALL SELECT 'NL_TRANSPORT_EMISSIONS', COUNT(*) FROM PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS
UNION ALL SELECT 'NL_TOTAL_EMISSIONS', COUNT(*) FROM PON_EV_LAB.CURATED.NL_TOTAL_EMISSIONS
UNION ALL SELECT 'EV_WEATHER_EMISSIONS', COUNT(*) FROM PON_EV_LAB.ANALYTICS.EV_WEATHER_EMISSIONS
UNION ALL SELECT 'REGIONAL_WEATHER_EV_CORRELATION', COUNT(*) FROM PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION;

-- Query the key regional correlation
SELECT * FROM PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION;
