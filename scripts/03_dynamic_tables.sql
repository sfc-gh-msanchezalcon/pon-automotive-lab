/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 3: Dynamic Tables Pipeline
=============================================================================
Dynamic Tables provide declarative, automatically refreshing pipelines.
No Airflow, no Data Factory, no cron jobs - just SQL.
=============================================================================
*/

USE DATABASE PON_EV_LAB;

-- =============================================================================
-- CURATED LAYER: Vehicles with Fuel Classification
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Vehicles joined with fuel types, EV classification applied'
AS
SELECT 
    v.kenteken,
    TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD') AS registration_date,
    YEAR(TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD')) AS registration_year,
    v.merk AS brand,
    v.handelsbenaming AS model,
    v.voertuigsoort AS vehicle_type,
    f.brandstof_omschrijving AS fuel_type,
    
    -- Classify fuel types into standard categories
    CASE 
        WHEN f.brandstof_omschrijving ILIKE '%elektr%' THEN 'Electric'
        WHEN f.brandstof_omschrijving ILIKE '%hybride%' THEN 'Hybrid'
        WHEN f.brandstof_omschrijving ILIKE '%waterstof%' THEN 'Hydrogen'
        WHEN f.brandstof_omschrijving ILIKE '%benzine%' THEN 'Petrol'
        WHEN f.brandstof_omschrijving ILIKE '%diesel%' THEN 'Diesel'
        WHEN f.brandstof_omschrijving ILIKE '%lpg%' THEN 'LPG'
        WHEN f.brandstof_omschrijving ILIKE '%cng%' THEN 'CNG'
        ELSE 'Other'
    END AS fuel_category,
    
    -- Boolean flag for EV/Hybrid vehicles
    CASE 
        WHEN f.brandstof_omschrijving ILIKE '%elektr%' 
          OR f.brandstof_omschrijving ILIKE '%hybride%'
          OR f.brandstof_omschrijving ILIKE '%waterstof%' 
        THEN TRUE 
        ELSE FALSE 
    END AS is_ev_or_hybrid
    
FROM RAW.VEHICLES_RAW v
LEFT JOIN RAW.VEHICLES_FUEL_RAW f ON v.kenteken = f.kenteken;

-- =============================================================================
-- CURATED LAYER: Charging Infrastructure by Area
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.CHARGING_BY_AREA
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Charging infrastructure aggregated by postal code area'
AS
SELECT 
    p.zipcode,
    LEFT(p.zipcode, 2) AS postal_area,
    LEFT(p.zipcode, 4) AS postal_code_4,
    COUNT(DISTINCT p.areaid) AS num_parking_locations,
    SUM(TRY_TO_NUMBER(c.chargingpointcapacity)) AS total_charging_points,
    AVG(TRY_TO_NUMBER(c.chargingpointcapacity)) AS avg_charging_per_location
FROM RAW.PARKING_ADDRESS_RAW p
LEFT JOIN RAW.CHARGING_CAPACITY_RAW c 
    ON p.areaid = c.areaid 
    AND p.areamanagerid = c.areamanagerid
WHERE p.zipcode IS NOT NULL
GROUP BY p.zipcode, LEFT(p.zipcode, 2), LEFT(p.zipcode, 4);

-- =============================================================================
-- ANALYTICS LAYER: EV Growth Trends
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'EV registration trends by year and fuel category'
AS
SELECT 
    registration_year,
    fuel_category,
    COUNT(*) AS vehicle_count,
    SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_hybrid_count,
    ROUND(SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(*), 0), 2) AS ev_percentage
FROM CURATED.VEHICLES_WITH_FUEL
WHERE registration_year IS NOT NULL 
  AND registration_year >= 2015
GROUP BY registration_year, fuel_category
ORDER BY registration_year, fuel_category;

-- =============================================================================
-- ANALYTICS LAYER: Year-over-Year EV Growth
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Year-over-year EV growth metrics'
AS
WITH yearly_totals AS (
    SELECT 
        registration_year,
        COUNT(*) AS total_vehicles,
        SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_count,
        SUM(CASE WHEN fuel_category = 'Electric' THEN 1 ELSE 0 END) AS pure_ev_count,
        SUM(CASE WHEN fuel_category = 'Hybrid' THEN 1 ELSE 0 END) AS hybrid_count
    FROM CURATED.VEHICLES_WITH_FUEL
    WHERE registration_year IS NOT NULL 
      AND registration_year >= 2015
    GROUP BY registration_year
)
SELECT 
    registration_year,
    total_vehicles,
    ev_count,
    pure_ev_count,
    hybrid_count,
    ROUND(ev_count * 100.0 / NULLIF(total_vehicles, 0), 2) AS ev_share_percent,
    LAG(ev_count) OVER (ORDER BY registration_year) AS prev_year_ev_count,
    CASE 
        WHEN LAG(ev_count) OVER (ORDER BY registration_year) > 0 
        THEN ROUND(
            (ev_count - LAG(ev_count) OVER (ORDER BY registration_year)) * 100.0 / 
            LAG(ev_count) OVER (ORDER BY registration_year), 1)
        ELSE NULL 
    END AS yoy_growth_percent
FROM yearly_totals
ORDER BY registration_year;

-- =============================================================================
-- Verify Dynamic Tables
-- =============================================================================

SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;

-- Force initial refresh
ALTER DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL REFRESH;
ALTER DYNAMIC TABLE CURATED.CHARGING_BY_AREA REFRESH;
ALTER DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS REFRESH;
ALTER DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH REFRESH;

-- Query the results
SELECT * FROM ANALYTICS.EV_YOY_GROWTH ORDER BY registration_year;
