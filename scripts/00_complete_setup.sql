/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Complete Setup Script (Modules 1-5)
=============================================================================
This file contains all SQL from Modules 1-5 for facilitator use.
Run this to set up the entire lab environment in one execution.

NOTE: Module 6 (Marketplace) requires manual steps in Snowsight UI:
  1. Navigate to Data Products > Marketplace
  2. Search and get: "Global Weather & Climate Data for BI" (Weather Source)
  3. Search and get: "Snowflake Public Data (Free)" (Snowflake)
  
NOTE: Module 7 (Streamlit) - Copy streamlit_app.py code into Snowsight
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

CREATE TABLE IF NOT EXISTS VEHICLES_RAW (
    kenteken STRING,
    datum_eerste_tenaamstelling_in_nederland STRING,
    merk STRING,
    handelsbenaming STRING,
    voertuigsoort STRING,
    raw_json VARIANT
);

CREATE TABLE IF NOT EXISTS VEHICLES_FUEL_RAW (
    kenteken STRING,
    brandstof_omschrijving STRING,
    raw_json VARIANT
);

CREATE TABLE IF NOT EXISTS PARKING_ADDRESS_RAW (
    areaid STRING,
    areamanagerid STRING,
    parkingaddresstype STRING,
    zipcode STRING,
    raw_json VARIANT
);

CREATE TABLE IF NOT EXISTS CHARGING_CAPACITY_RAW (
    areaid STRING,
    areamanagerid STRING,
    chargingpointcapacity STRING,
    raw_json VARIANT
);

-- =============================================================================
-- MODULE 2: External API Data Ingestion
-- =============================================================================

CREATE OR REPLACE NETWORK RULE rdw_api_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('opendata.rdw.nl:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION rdw_api_access
    ALLOWED_NETWORK_RULES = (rdw_api_rule)
    ENABLED = TRUE;

CREATE OR REPLACE FUNCTION FETCH_RDW_DATA(dataset_id STRING, row_limit INT, row_offset INT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (rdw_api_access)
HANDLER = 'fetch_data'
AS $$
import requests
def fetch_data(dataset_id, row_limit, row_offset):
    url = f"https://opendata.rdw.nl/resource/{dataset_id}.json"
    params = {"$limit": row_limit, "$offset": row_offset}
    response = requests.get(url, params=params, timeout=60)
    return response.json()
$$;

-- Load vehicles
INSERT INTO VEHICLES_RAW (kenteken, datum_eerste_tenaamstelling_in_nederland, merk, handelsbenaming, voertuigsoort, raw_json)
WITH offsets AS (SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val FROM TABLE(GENERATOR(ROWCOUNT => 50))),
api_data AS (SELECT FETCH_RDW_DATA('m9d7-ebf2', 1000, offset_val) AS data FROM offsets)
SELECT f.value:kenteken::STRING, f.value:datum_eerste_tenaamstelling_in_nederland::STRING, f.value:merk::STRING, f.value:handelsbenaming::STRING, f.value:voertuigsoort::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- Load fuel
INSERT INTO VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving, raw_json)
WITH offsets AS (SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val FROM TABLE(GENERATOR(ROWCOUNT => 150))),
api_data AS (SELECT FETCH_RDW_DATA('8ys7-d773', 1000, offset_val) AS data FROM offsets)
SELECT f.value:kenteken::STRING, f.value:brandstof_omschrijving::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- Load parking
INSERT INTO PARKING_ADDRESS_RAW (areaid, areamanagerid, parkingaddresstype, zipcode, raw_json)
WITH api_data AS (SELECT FETCH_RDW_DATA('ygq4-hh5q', 10000, 0) AS data)
SELECT f.value:areaid::STRING, f.value:areamanagerid::STRING, f.value:parkingaddresstype::STRING, f.value:zipcode::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f WHERE f.value:parkingaddresstype::STRING = 'F';

-- Load charging
INSERT INTO CHARGING_CAPACITY_RAW (areaid, areamanagerid, chargingpointcapacity, raw_json)
WITH api_data AS (SELECT FETCH_RDW_DATA('b3us-f26s', 10000, 0) AS data)
SELECT f.value:areaid::STRING, f.value:areamanagerid::STRING, f.value:chargingpointcapacity::STRING, f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- =============================================================================
-- MODULE 3: Dynamic Tables
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL TARGET_LAG = '1 hour' WAREHOUSE = COMPUTE_WH AS
SELECT v.kenteken, TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD') AS registration_date,
    YEAR(TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD')) AS registration_year,
    v.merk AS brand, v.handelsbenaming AS model, v.voertuigsoort AS vehicle_type, f.brandstof_omschrijving AS fuel_type,
    CASE WHEN f.brandstof_omschrijving ILIKE '%elektr%' THEN 'Electric' WHEN f.brandstof_omschrijving ILIKE '%hybride%' THEN 'Hybrid'
        WHEN f.brandstof_omschrijving ILIKE '%waterstof%' THEN 'Hydrogen' WHEN f.brandstof_omschrijving ILIKE '%benzine%' THEN 'Petrol'
        WHEN f.brandstof_omschrijving ILIKE '%diesel%' THEN 'Diesel' WHEN f.brandstof_omschrijving ILIKE '%lpg%' THEN 'LPG' ELSE 'Other' END AS fuel_category,
    CASE WHEN f.brandstof_omschrijving ILIKE '%elektr%' OR f.brandstof_omschrijving ILIKE '%hybride%' OR f.brandstof_omschrijving ILIKE '%waterstof%' THEN TRUE ELSE FALSE END AS is_ev_or_hybrid
FROM RAW.VEHICLES_RAW v LEFT JOIN RAW.VEHICLES_FUEL_RAW f ON v.kenteken = f.kenteken;

CREATE OR REPLACE DYNAMIC TABLE CURATED.CHARGING_BY_AREA TARGET_LAG = '1 hour' WAREHOUSE = COMPUTE_WH AS
SELECT p.zipcode, LEFT(p.zipcode, 2) AS postal_area, COUNT(DISTINCT p.areaid) AS num_parking_locations,
    SUM(TRY_TO_NUMBER(c.chargingpointcapacity)) AS total_charging_points, AVG(TRY_TO_NUMBER(c.chargingpointcapacity)) AS avg_charging_per_location
FROM RAW.PARKING_ADDRESS_RAW p LEFT JOIN RAW.CHARGING_CAPACITY_RAW c ON p.areaid = c.areaid AND p.areamanagerid = c.areamanagerid
WHERE p.zipcode IS NOT NULL GROUP BY p.zipcode, LEFT(p.zipcode, 2);

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS TARGET_LAG = '1 hour' WAREHOUSE = COMPUTE_WH AS
SELECT registration_year, fuel_category, COUNT(*) AS vehicle_count, SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_hybrid_count,
    ROUND(SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS ev_percentage
FROM CURATED.VEHICLES_WITH_FUEL WHERE registration_year IS NOT NULL AND registration_year >= 2015 GROUP BY registration_year, fuel_category;

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH TARGET_LAG = '1 hour' WAREHOUSE = COMPUTE_WH AS
WITH yearly AS (SELECT registration_year, COUNT(*) AS total_vehicles, SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_count
    FROM CURATED.VEHICLES_WITH_FUEL WHERE registration_year IS NOT NULL AND registration_year >= 2015 GROUP BY registration_year)
SELECT registration_year, total_vehicles, ev_count, ROUND(ev_count * 100.0 / NULLIF(total_vehicles, 0), 2) AS ev_share_percent,
    LAG(ev_count) OVER (ORDER BY registration_year) AS prev_year_ev_count,
    CASE WHEN LAG(ev_count) OVER (ORDER BY registration_year) > 0 THEN ROUND((ev_count - LAG(ev_count) OVER (ORDER BY registration_year)) * 100.0 / LAG(ev_count) OVER (ORDER BY registration_year), 1) ELSE NULL END AS yoy_growth_percent
FROM yearly ORDER BY registration_year;

-- =============================================================================
-- MODULE 4: Scaling and Cost Control
-- =============================================================================

CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD' INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE RESOURCE MONITOR PON_LAB_MONITOR WITH CREDIT_QUOTA = 100 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 50 PERCENT DO NOTIFY ON 75 PERCENT DO NOTIFY ON 90 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE PON_ANALYTICS_WH SET RESOURCE_MONITOR = PON_LAB_MONITOR;

-- Update Dynamic Tables to use our warehouse
ALTER DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE CURATED.CHARGING_BY_AREA SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH SET WAREHOUSE = PON_ANALYTICS_WH;

-- =============================================================================
-- MODULE 5: Secure Data Sharing
-- =============================================================================

CREATE OR REPLACE SHARE PON_DEALER_SHARE COMMENT = 'EV analytics for Pon dealer network';
GRANT USAGE ON DATABASE PON_EV_LAB TO SHARE PON_DEALER_SHARE;
GRANT USAGE ON SCHEMA ANALYTICS TO SHARE PON_DEALER_SHARE;
GRANT SELECT ON TABLE ANALYTICS.EV_GROWTH_TRENDS TO SHARE PON_DEALER_SHARE;
GRANT SELECT ON TABLE ANALYTICS.EV_YOY_GROWTH TO SHARE PON_DEALER_SHARE;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'Setup Complete!' AS status;
SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;
SHOW SHARES LIKE 'PON%';

SELECT 'VEHICLES_RAW' AS tbl, COUNT(*) AS cnt FROM RAW.VEHICLES_RAW UNION ALL
SELECT 'VEHICLES_FUEL_RAW', COUNT(*) FROM RAW.VEHICLES_FUEL_RAW UNION ALL
SELECT 'PARKING_ADDRESS_RAW', COUNT(*) FROM RAW.PARKING_ADDRESS_RAW UNION ALL
SELECT 'CHARGING_CAPACITY_RAW', COUNT(*) FROM RAW.CHARGING_CAPACITY_RAW;
