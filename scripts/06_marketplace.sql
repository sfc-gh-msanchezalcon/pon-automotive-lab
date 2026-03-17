/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 6: Marketplace Data Enrichment
=============================================================================
PREREQUISITE: Get these FREE datasets from Snowflake Marketplace first:

1. "Global Weather & Climate Data for BI" from Pelmorex Weather Source
   → Database name: WEATHER_SOURCE

2. "Snowflake Public Data (Free)" from Snowflake Public Data Products
   → Database name: SNOWFLAKE_PUBLIC_DATA

To get datasets:
  1. Navigate to Data Products > Marketplace
  2. Search for the dataset name
  3. Click "Get" and accept terms
=============================================================================
*/

-- ============================================
-- Example 1: Weather Impact on EV Range
-- ============================================
-- Cold weather reduces EV battery range by 20-40%

-- Check available cities in Netherlands
SELECT DISTINCT city_name, country
FROM WEATHER_SOURCE.STANDARD_TILE.HISTORY_DAY
WHERE country = 'NL'
LIMIT 10;

-- Monthly temperature trends
SELECT 
    DATE_TRUNC('month', date_valid_std) AS month,
    ROUND(AVG(avg_temperature_air_2m_f), 1) AS avg_temp_f,
    ROUND((AVG(avg_temperature_air_2m_f) - 32) * 5/9, 1) AS avg_temp_c,
    ROUND(AVG(tot_precipitation_in), 2) AS avg_precipitation_in
FROM WEATHER_SOURCE.STANDARD_TILE.HISTORY_DAY
WHERE country = 'NL'
    AND date_valid_std >= '2023-01-01'
GROUP BY 1
ORDER BY 1;

-- ============================================
-- Example 2: Economic & Demographic Data
-- ============================================

-- Explore available economic timeseries for Netherlands
SELECT DISTINCT 
    ts.variable_name,
    ts.unit,
    g.geo_name
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.DATACOMMONS_TIMESERIES ts
JOIN SNOWFLAKE_PUBLIC_DATA.CYBERSYN.GEOGRAPHY_INDEX g 
    ON ts.geo_id = g.geo_id
WHERE g.geo_name ILIKE '%Netherlands%'
LIMIT 20;

-- GDP and economic indicators
SELECT 
    date,
    variable_name,
    value,
    unit
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.OECD_TIMESERIES
WHERE geo ILIKE '%Netherlands%'
    AND variable_name ILIKE '%GDP%'
    AND date >= '2020-01-01'
ORDER BY date DESC
LIMIT 20;

-- ============================================
-- Example 3: Energy Prices
-- ============================================
-- Electricity costs directly impact EV ownership economics

-- EU Electricity prices
SELECT 
    date,
    geo,
    variable_name,
    value,
    unit
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.OECD_TIMESERIES
WHERE variable_name ILIKE '%electricity%price%'
    AND geo ILIKE '%Netherlands%'
    AND date >= '2020-01-01'
ORDER BY date DESC
LIMIT 20;

-- ============================================
-- Example 4: Emissions Data
-- ============================================
-- Track progress toward climate goals

SELECT 
    date,
    variable_name,
    value,
    unit
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.CLIMATE_WATCH_TIMESERIES
WHERE geo ILIKE '%Netherlands%'
    AND variable_name ILIKE '%CO2%'
ORDER BY date DESC
LIMIT 20;

-- ============================================
-- Verification
-- ============================================
SELECT 'Marketplace datasets ready!' AS status;
