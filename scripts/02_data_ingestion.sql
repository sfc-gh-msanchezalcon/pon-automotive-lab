/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 2: External API Data Ingestion
=============================================================================
This module demonstrates Snowflake's External Access capability - calling
external APIs directly from SQL without any external tools.
=============================================================================
*/

USE DATABASE PON_EV_LAB;
USE SCHEMA RAW;

-- =============================================================================
-- STEP 1: Network Configuration
-- =============================================================================

-- Create network rule to allow outbound calls to RDW APIs
CREATE OR REPLACE NETWORK RULE rdw_api_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('opendata.rdw.nl:443')
    COMMENT = 'Allow HTTPS access to RDW Open Data APIs';

-- Create external access integration using the network rule
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION rdw_api_access
    ALLOWED_NETWORK_RULES = (rdw_api_rule)
    ENABLED = TRUE
    COMMENT = 'External access for RDW Open Data API calls';

-- =============================================================================
-- STEP 2: Python UDF for API Fetching
-- =============================================================================

CREATE OR REPLACE FUNCTION FETCH_RDW_DATA(
    dataset_id STRING,
    row_limit INT,
    row_offset INT
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (rdw_api_access)
HANDLER = 'fetch_data'
COMMENT = 'Fetches paginated data from RDW Open Data Socrata API'
AS $$
import requests

def fetch_data(dataset_id, row_limit, row_offset):
    """
    Fetch data from RDW Open Data API with pagination.
    
    Dataset IDs:
    - m9d7-ebf2: Gekentekende voertuigen (registered vehicles)
    - 8ys7-d773: Brandstof (fuel types)  
    - 8wbe-pu7d: Voertuigen per postcode (vehicles by postal code - KEY dataset!)
    - ygq4-hh5q: Parkeerlocaties (parking locations)
    - b3us-f26s: Laadpalen capaciteit (charging capacity)
    """
    url = f"https://opendata.rdw.nl/resource/{dataset_id}.json"
    params = {
        "$limit": row_limit,
        "$offset": row_offset
    }
    
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    
    return response.json()
$$;

-- =============================================================================
-- STEP 3: Test API Connection
-- =============================================================================

-- Test: Fetch 5 vehicle records
SELECT FETCH_RDW_DATA('m9d7-ebf2', 5, 0) AS sample_vehicles;

-- Test: Fetch 5 fuel records
SELECT FETCH_RDW_DATA('8ys7-d773', 5, 0) AS sample_fuel;

-- =============================================================================
-- STEP 4: Load Vehicle Data (50,000 records)
-- =============================================================================

INSERT INTO VEHICLES_RAW 
    (kenteken, datum_eerste_tenaamstelling_in_nederland, merk, handelsbenaming, voertuigsoort, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT FETCH_RDW_DATA('m9d7-ebf2', 1000, offset_val) AS data
    FROM offsets
)
SELECT 
    f.value:kenteken::STRING,
    f.value:datum_eerste_tenaamstelling_in_nederland::STRING,
    f.value:merk::STRING,
    f.value:handelsbenaming::STRING,
    f.value:voertuigsoort::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- =============================================================================
-- STEP 5: Load Fuel Data (150,000 records for better join coverage)
-- =============================================================================

INSERT INTO VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 150))
),
api_data AS (
    SELECT FETCH_RDW_DATA('8ys7-d773', 1000, offset_val) AS data
    FROM offsets
)
SELECT 
    f.value:kenteken::STRING,
    f.value:brandstof_omschrijving::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- =============================================================================
-- STEP 6: Load Parking Data
-- =============================================================================

INSERT INTO PARKING_ADDRESS_RAW 
    (areaid, areamanagerid, parkingaddresstype, zipcode, raw_json)
WITH api_data AS (
    SELECT FETCH_RDW_DATA('ygq4-hh5q', 10000, 0) AS data
)
SELECT 
    f.value:areaid::STRING,
    f.value:areamanagerid::STRING,
    f.value:parkingaddresstype::STRING,
    f.value:zipcode::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f
WHERE f.value:parkingaddresstype::STRING = 'F';  -- Filter for facility type

-- =============================================================================
-- STEP 7: Load Charging Capacity Data
-- =============================================================================

INSERT INTO CHARGING_CAPACITY_RAW 
    (areaid, areamanagerid, chargingpointcapacity, raw_json)
WITH api_data AS (
    SELECT FETCH_RDW_DATA('b3us-f26s', 10000, 0) AS data
)
SELECT 
    f.value:areaid::STRING,
    f.value:areamanagerid::STRING,
    f.value:chargingpointcapacity::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- =============================================================================
-- STEP 8: Load Vehicles by Postcode (KEY dataset for regional analysis)
-- =============================================================================
-- This is the critical dataset for answering "which region has fastest EV growth"
-- Contains: postcode, vehicle type, fuel type (B/D/E), plug-in capable (Y/N), count

INSERT INTO VEHICLES_BY_POSTCODE_RAW 
    (postcode, voertuigsoort, brandstof, extern_oplaadbaar, aantal, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT FETCH_RDW_DATA('8wbe-pu7d', 1000, offset_val) AS data
    FROM offsets
)
SELECT 
    f.value:postcode::STRING,
    f.value:voertuigsoort::STRING,
    f.value:brandstof::STRING,
    f.value:extern_oplaadbaar::STRING,
    f.value:aantal::INT,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;

-- =============================================================================
-- STEP 9: Verify Data Load
-- =============================================================================

SELECT 'VEHICLES_RAW' AS table_name, COUNT(*) AS row_count FROM VEHICLES_RAW
UNION ALL SELECT 'VEHICLES_FUEL_RAW', COUNT(*) FROM VEHICLES_FUEL_RAW
UNION ALL SELECT 'VEHICLES_BY_POSTCODE_RAW', COUNT(*) FROM VEHICLES_BY_POSTCODE_RAW
UNION ALL SELECT 'PARKING_ADDRESS_RAW', COUNT(*) FROM PARKING_ADDRESS_RAW
UNION ALL SELECT 'CHARGING_CAPACITY_RAW', COUNT(*) FROM CHARGING_CAPACITY_RAW;

-- Check fuel type distribution
SELECT brandstof_omschrijving, COUNT(*) AS count
FROM VEHICLES_FUEL_RAW
GROUP BY brandstof_omschrijving
ORDER BY count DESC;
