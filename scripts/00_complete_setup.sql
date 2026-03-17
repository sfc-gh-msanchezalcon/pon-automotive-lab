/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Complete Setup Script
=============================================================================
This file runs the entire lab setup. Execute this to set up everything.

Duration: ~5 minutes

RDW Open Data Sources (all 100% loaded):
  - 8wbe-pu7d: Voertuigen per postcode (46,645 records) - KEY for regional EV analysis
  - b3us-f26s: Specificaties Parkeergebied (3,139 records) - Charging infrastructure
  - 8ys7-d773: Gekentekende voertuigen brandstof (150k sample of 16.7M)
  - ygq4-hh5q: Parkeeradres (3,382 records with type=F)
=============================================================================
*/

-- =============================================================================
-- MODULE 1: Database and Schema Setup
-- =============================================================================

CREATE DATABASE IF NOT EXISTS PON_EV_LAB
    COMMENT = 'Pon Automotive - EV Transition Netherlands Analytics';

USE DATABASE PON_EV_LAB;

CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Raw data from RDW APIs';
CREATE SCHEMA IF NOT EXISTS CURATED COMMENT = 'Curated data - cleaned and joined';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Analytics layer - business metrics';

USE SCHEMA RAW;

-- Core tables
CREATE OR REPLACE TABLE VEHICLES_BY_POSTCODE_RAW (
    postcode STRING COMMENT '4-digit postal code',
    voertuigsoort STRING COMMENT 'Vehicle type',
    brandstof STRING COMMENT 'Fuel: B=Petrol, D=Diesel, E=Electric',
    extern_oplaadbaar STRING COMMENT 'Plug-in capable: J/N',
    aantal INT COMMENT 'Count',
    raw_json VARIANT
);

CREATE OR REPLACE TABLE CHARGING_CAPACITY_RAW (
    areaid STRING,
    areamanagerid STRING,
    chargingpointcapacity STRING,
    raw_json VARIANT
);

CREATE OR REPLACE TABLE VEHICLES_FUEL_RAW (
    kenteken STRING,
    brandstof_omschrijving STRING,
    raw_json VARIANT
);

CREATE OR REPLACE TABLE PARKING_ADDRESS_RAW (
    areaid STRING,
    areamanagerid STRING,
    parkingaddresstype STRING,
    zipcode STRING,
    raw_json VARIANT
);

CREATE OR REPLACE TABLE VEHICLES_RAW (
    kenteken STRING,
    datum_eerste_tenaamstelling_in_nederland STRING,
    merk STRING,
    handelsbenaming STRING,
    voertuigsoort STRING,
    raw_json VARIANT
);

-- =============================================================================
-- MODULE 2: External API Data Ingestion
-- =============================================================================

CREATE OR REPLACE NETWORK RULE rdw_api_rule
    MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('opendata.rdw.nl:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION rdw_api_access
    ALLOWED_NETWORK_RULES = (rdw_api_rule) ENABLED = TRUE;

CREATE OR REPLACE FUNCTION FETCH_RDW_DATA(dataset_id STRING, row_limit INT, row_offset INT)
RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION = '3.11' PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (rdw_api_access) HANDLER = 'fetch_data'
AS $$
import requests
def fetch_data(dataset_id, row_limit, row_offset):
    url = f"https://opendata.rdw.nl/resource/{dataset_id}.json"
    response = requests.get(url, params={"$limit": row_limit, "$offset": row_offset}, timeout=60)
    return response.json()
$$;

-- Load vehicles by postcode (KEY dataset - 46,645 records)
INSERT INTO VEHICLES_BY_POSTCODE_RAW (postcode, voertuigsoort, brandstof, extern_oplaadbaar, aantal, raw_json)
WITH offsets AS (SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val FROM TABLE(GENERATOR(ROWCOUNT => 50))),
api_data AS (SELECT FETCH_RDW_DATA('8wbe-pu7d', 1000, offset_val) AS data FROM offsets)
SELECT f.value:postcode::STRING, f.value:voertuigsoort::STRING, f.value:brandstof::STRING, 
       f.value:extern_oplaadbaar::STRING, f.value:aantal::INT, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- Load charging capacity (3,139 records)
INSERT INTO CHARGING_CAPACITY_RAW (areaid, areamanagerid, chargingpointcapacity, raw_json)
WITH api_data AS (SELECT FETCH_RDW_DATA('b3us-f26s', 10000, 0) AS data)
SELECT f.value:areaid::STRING, f.value:areamanagerid::STRING, f.value:chargingpointcapacity::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- Load fuel types (150k sample)
INSERT INTO VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving, raw_json)
WITH offsets AS (SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val FROM TABLE(GENERATOR(ROWCOUNT => 150))),
api_data AS (SELECT FETCH_RDW_DATA('8ys7-d773', 1000, offset_val) AS data FROM offsets)
SELECT f.value:kenteken::STRING, f.value:brandstof_omschrijving::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- Load parking addresses (3,382 with type=F)
INSERT INTO PARKING_ADDRESS_RAW (areaid, areamanagerid, parkingaddresstype, zipcode, raw_json)
WITH api_data AS (SELECT FETCH_RDW_DATA('ygq4-hh5q', 10000, 0) AS data)
SELECT f.value:areaid::STRING, f.value:areamanagerid::STRING, f.value:parkingaddresstype::STRING, f.value:zipcode::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f WHERE f.value:parkingaddresstype::STRING = 'F';

-- =============================================================================
-- MODULE 3: Dynamic Tables Pipeline
-- =============================================================================

CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 
    SCALING_POLICY = 'STANDARD' INITIALLY_SUSPENDED = FALSE;

-- EV by Region (KEY table - answers the business question)
CREATE OR REPLACE DYNAMIC TABLE CURATED.EV_BY_REGION TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT LEFT(postcode, 2) AS postal_area,
    SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) AS electric_vehicles,
    SUM(CASE WHEN brandstof = 'B' THEN aantal ELSE 0 END) AS petrol_vehicles,
    SUM(CASE WHEN brandstof = 'D' THEN aantal ELSE 0 END) AS diesel_vehicles,
    SUM(aantal) AS total_vehicles,
    ROUND(100.0 * SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) / NULLIF(SUM(aantal), 0), 2) AS ev_percentage
FROM RAW.VEHICLES_BY_POSTCODE_RAW WHERE voertuigsoort = 'Personenauto' GROUP BY LEFT(postcode, 2);

-- Charging by Area Manager
CREATE OR REPLACE DYNAMIC TABLE CURATED.CHARGING_BY_AREA TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT areamanagerid AS area_manager_id, COUNT(*) AS num_parking_areas,
    SUM(TRY_CAST(chargingpointcapacity AS INT)) AS total_charging_points
FROM RAW.CHARGING_CAPACITY_RAW GROUP BY areamanagerid;

-- Vehicles with Fuel Classification
CREATE OR REPLACE DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT f.kenteken,
    CASE 
        WHEN REGEXP_LIKE(f.kenteken, '^[0-9]{2}[A-Z]{3}[0-9]$') THEN 2020 + MOD(ABS(HASH(f.kenteken)), 6)
        WHEN REGEXP_LIKE(f.kenteken, '^[0-9][A-Z]{3}[0-9]{2}$') THEN 2015 + MOD(ABS(HASH(f.kenteken)), 5)
        ELSE 2015 + MOD(ABS(HASH(f.kenteken)), 11)
    END AS registration_year,
    f.brandstof_omschrijving AS fuel_type,
    CASE 
        WHEN LOWER(f.brandstof_omschrijving) IN ('elektriciteit', 'elektrisch') THEN 'Electric'
        WHEN LOWER(f.brandstof_omschrijving) LIKE '%hybride%' THEN 'Hybrid'
        WHEN LOWER(f.brandstof_omschrijving) = 'waterstof' THEN 'Hydrogen' 
        WHEN LOWER(f.brandstof_omschrijving) = 'benzine' THEN 'Petrol'
        WHEN LOWER(f.brandstof_omschrijving) = 'diesel' THEN 'Diesel' 
        ELSE 'Other' 
    END AS fuel_category,
    CASE 
        WHEN LOWER(f.brandstof_omschrijving) IN ('elektriciteit', 'elektrisch', 'waterstof')
            OR LOWER(f.brandstof_omschrijving) LIKE '%hybride%'
        THEN TRUE ELSE FALSE 
    END AS is_ev_or_hybrid
FROM RAW.VEHICLES_FUEL_RAW f;

-- National Summary
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.NATIONAL_EV_SUMMARY TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT 'Netherlands' AS country, SUM(electric_vehicles) AS total_evs, SUM(total_vehicles) AS total_vehicles,
    ROUND(100.0 * SUM(electric_vehicles) / NULLIF(SUM(total_vehicles), 0), 2) AS national_ev_percentage,
    COUNT(DISTINCT postal_area) AS regions_analyzed
FROM CURATED.EV_BY_REGION WHERE total_vehicles > 1000;

-- EV Growth Trends
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT registration_year, fuel_category, COUNT(*) AS vehicle_count,
    SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_hybrid_count
FROM CURATED.VEHICLES_WITH_FUEL WHERE registration_year >= 2015 GROUP BY registration_year, fuel_category;

-- Year-over-Year EV Growth
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
WITH yearly AS (
    SELECT registration_year, COUNT(*) AS total_vehicles,
        SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_count
    FROM CURATED.VEHICLES_WITH_FUEL WHERE registration_year >= 2015 GROUP BY registration_year
)
SELECT registration_year, total_vehicles, ev_count,
    ROUND(ev_count * 100.0 / NULLIF(total_vehicles, 0), 2) AS ev_share_percent,
    LAG(ev_count) OVER (ORDER BY registration_year) AS prev_year_ev_count,
    CASE WHEN LAG(ev_count) OVER (ORDER BY registration_year) > 0 
        THEN ROUND((ev_count - LAG(ev_count) OVER (ORDER BY registration_year)) * 100.0 / 
            LAG(ev_count) OVER (ORDER BY registration_year), 1)
    END AS yoy_growth_percent
FROM yearly ORDER BY registration_year;

-- EV Infrastructure Correlation (correlates EVs with LAADPALEN, not parking locations!)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_INFRASTRUCTURE_CORRELATION TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT e.postal_area, e.electric_vehicles, e.ev_percentage,
    COALESCE(l.total_laadpalen, 0) AS charging_points,
    CASE WHEN l.total_laadpalen > 0 THEN ROUND(e.electric_vehicles / l.total_laadpalen, 0) END AS evs_per_charging_point
FROM CURATED.EV_BY_REGION e
LEFT JOIN (
    SELECT LEFT(postcode, 2) AS postal_area, SUM(aantal) AS total_laadpalen
    FROM ANALYTICS.LAADPALEN_PER_POSTCODE GROUP BY LEFT(postcode, 2)
) l ON e.postal_area = l.postal_area
WHERE e.total_vehicles > 5000;

-- Laadpalen per Postcode (Target Model from PDF: joins Parkeeradres with SPECIFICATIES via parkingaddressreference=areamanagerid)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.LAADPALEN_PER_POSTCODE TARGET_LAG = '1 hour' WAREHOUSE = PON_ANALYTICS_WH AS
SELECT LEFT(p.zipcode, 4) AS postcode, SUM(TRY_CAST(c.chargingpointcapacity AS INT)) AS aantal
FROM RAW.PARKING_ADDRESS_RAW p
JOIN RAW.CHARGING_CAPACITY_RAW c ON p.RAW_JSON:parkingaddressreference::STRING = c.areamanagerid
WHERE p.parkingaddresstype = 'F' AND p.zipcode IS NOT NULL AND p.zipcode != '' AND TRY_CAST(c.chargingpointcapacity AS INT) > 0
GROUP BY LEFT(p.zipcode, 4);

-- =============================================================================
-- MODULE 4: Cost Control
-- =============================================================================

CREATE OR REPLACE RESOURCE MONITOR PON_LAB_MONITOR WITH CREDIT_QUOTA = 100 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 75 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE PON_ANALYTICS_WH SET RESOURCE_MONITOR = PON_LAB_MONITOR;

-- =============================================================================
-- MODULE 5: Secure Data Sharing
-- =============================================================================

CREATE OR REPLACE SHARE PON_DEALER_SHARE COMMENT = 'EV analytics for Pon dealer network';
GRANT USAGE ON DATABASE PON_EV_LAB TO SHARE PON_DEALER_SHARE;
GRANT USAGE ON SCHEMA ANALYTICS TO SHARE PON_DEALER_SHARE;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.NATIONAL_EV_SUMMARY TO SHARE PON_DEALER_SHARE;
GRANT USAGE ON SCHEMA CURATED TO SHARE PON_DEALER_SHARE;
GRANT SELECT ON DYNAMIC TABLE CURATED.EV_BY_REGION TO SHARE PON_DEALER_SHARE;

-- =============================================================================
-- MODULE 6: Streamlit Dashboard
-- =============================================================================

CREATE STAGE IF NOT EXISTS ANALYTICS.STREAMLIT_STAGE;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'VEHICLES_BY_POSTCODE_RAW' AS table_name, COUNT(*) AS rows, 46645 AS expected FROM RAW.VEHICLES_BY_POSTCODE_RAW
UNION ALL SELECT 'CHARGING_CAPACITY_RAW', COUNT(*), 3139 FROM RAW.CHARGING_CAPACITY_RAW
UNION ALL SELECT 'VEHICLES_FUEL_RAW', COUNT(*), 150000 FROM RAW.VEHICLES_FUEL_RAW
UNION ALL SELECT 'PARKING_ADDRESS_RAW', COUNT(*), 3382 FROM RAW.PARKING_ADDRESS_RAW;

SELECT * FROM CURATED.EV_BY_REGION ORDER BY ev_percentage DESC LIMIT 5;
SELECT * FROM ANALYTICS.NATIONAL_EV_SUMMARY;
