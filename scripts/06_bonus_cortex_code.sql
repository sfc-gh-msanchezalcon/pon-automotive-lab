-- =============================================================================
-- Module 9: Bonus - Fleet Telemetry Pipeline
-- =============================================================================
-- This script contains the SQL that Cortex Code generates from natural language
-- prompts. Use this as a reference or fallback if Cortex Code is unavailable.
-- =============================================================================

USE DATABASE PON_EV_LAB;
USE WAREHOUSE PON_ANALYTICS_WH;

-- -----------------------------------------------------------------------------
-- 9.2 Raw Telemetry Table
-- Prompt: "Create a table to store EV telemetry events..."
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE PON_EV_LAB.RAW.TELEMETRY_RAW (
    vin STRING COMMENT 'Vehicle identification (e.g., VW-EV-001)',
    event_timestamp TIMESTAMP_NTZ COMMENT 'When the telemetry was recorded',
    battery_pct INT COMMENT 'Battery percentage (0-100)',
    latitude FLOAT COMMENT 'GPS latitude',
    longitude FLOAT COMMENT 'GPS longitude',
    speed_kmh INT COMMENT 'Current speed in km/h',
    charging BOOLEAN COMMENT 'Whether vehicle is currently charging',
    raw_json VARIANT COMMENT 'Complete telemetry payload'
);

-- -----------------------------------------------------------------------------
-- 9.3 Synthetic Data Generation
-- Prompt: "Insert 1000 rows of synthetic telemetry data..."
-- -----------------------------------------------------------------------------
INSERT INTO PON_EV_LAB.RAW.TELEMETRY_RAW 
WITH vehicles AS (
    -- VW: 50 vehicles
    SELECT 'VW-EV-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::STRING, 3, '0') AS vin 
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
    UNION ALL
    -- Tesla: 30 vehicles
    SELECT 'TESLA-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::STRING, 3, '0') AS vin 
    FROM TABLE(GENERATOR(ROWCOUNT => 30))
    UNION ALL
    -- BMW: 20 vehicles
    SELECT 'BMW-EV-' || LPAD(ROW_NUMBER() OVER (ORDER BY SEQ4())::STRING, 3, '0') AS vin 
    FROM TABLE(GENERATOR(ROWCOUNT => 20))
),
events AS (
    SELECT 
        v.vin,
        DATEADD('minute', -UNIFORM(0, 1440, RANDOM()), CURRENT_TIMESTAMP()) AS event_timestamp,
        UNIFORM(10, 100, RANDOM()) AS battery_pct,
        UNIFORM(51.0, 53.5, RANDOM())::FLOAT AS latitude,  -- Netherlands bounds
        UNIFORM(3.5, 7.0, RANDOM())::FLOAT AS longitude,
        UNIFORM(0, 130, RANDOM()) AS speed_kmh,
        UNIFORM(0, 10, RANDOM()) < 2 AS charging  -- 20% chance charging
    FROM vehicles v,
    TABLE(GENERATOR(ROWCOUNT => 10))  -- 10 events per vehicle = 1000 total
)
SELECT 
    vin,
    event_timestamp,
    battery_pct,
    latitude,
    longitude,
    speed_kmh,
    charging,
    OBJECT_CONSTRUCT(
        'vin', vin, 
        'battery', battery_pct, 
        'speed', speed_kmh,
        'location', OBJECT_CONSTRUCT('lat', latitude, 'lon', longitude)
    ) AS raw_json
FROM events;

-- Verify data load
SELECT 'Telemetry rows loaded' AS status, COUNT(*) AS count 
FROM PON_EV_LAB.RAW.TELEMETRY_RAW;

-- -----------------------------------------------------------------------------
-- 9.4 Dynamic Table for Current Vehicle Status
-- Prompt: "Create a Dynamic Table showing current status of each vehicle..."
-- -----------------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.VEHICLE_STATUS
    TARGET_LAG = '1 minute'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Current status of each vehicle in the fleet'
AS
SELECT 
    vin,
    SPLIT_PART(vin, '-', 1) AS brand,
    MAX(event_timestamp) AS last_seen,
    MAX_BY(battery_pct, event_timestamp) AS current_battery,
    MAX_BY(latitude, event_timestamp) AS current_lat,
    MAX_BY(longitude, event_timestamp) AS current_lon,
    ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh,
    CASE 
        WHEN MAX_BY(battery_pct, event_timestamp) < 20 THEN 'LOW_BATTERY'
        WHEN MAX_BY(charging, event_timestamp) THEN 'CHARGING'
        ELSE 'ACTIVE'
    END AS status
FROM PON_EV_LAB.RAW.TELEMETRY_RAW
WHERE event_timestamp > DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY vin;

-- -----------------------------------------------------------------------------
-- 9.5 Analytics View for Fleet Summary
-- Prompt: "Create a view that summarizes the fleet by brand..."
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW PON_EV_LAB.ANALYTICS.FLEET_ALERTS 
    COMMENT = 'Fleet monitoring dashboard - alerts and KPIs by brand'
AS
SELECT 
    brand,
    COUNT(*) AS total_vehicles,
    COUNT(CASE WHEN status = 'LOW_BATTERY' THEN 1 END) AS low_battery_count,
    COUNT(CASE WHEN status = 'CHARGING' THEN 1 END) AS charging_count,
    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count,
    ROUND(AVG(current_battery), 0) AS avg_battery_pct,
    ROUND(AVG(avg_speed_kmh), 1) AS avg_fleet_speed
FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
GROUP BY brand
ORDER BY total_vehicles DESC;

-- -----------------------------------------------------------------------------
-- 9.6 Verification Queries
-- -----------------------------------------------------------------------------

-- Fleet status overview
SELECT * FROM PON_EV_LAB.CURATED.VEHICLE_STATUS 
ORDER BY last_seen DESC 
LIMIT 20;

-- Fleet alerts by brand
SELECT * FROM PON_EV_LAB.ANALYTICS.FLEET_ALERTS;

-- Vehicles needing attention
SELECT vin, brand, current_battery, status 
FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
WHERE status IN ('LOW_BATTERY', 'CHARGING')
ORDER BY current_battery;

-- Summary
SELECT 
    'Total Vehicles' AS metric, COUNT(*)::STRING AS value FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
UNION ALL
SELECT 'Low Battery Alerts', COUNT(*)::STRING FROM PON_EV_LAB.CURATED.VEHICLE_STATUS WHERE status = 'LOW_BATTERY'
UNION ALL
SELECT 'Currently Charging', COUNT(*)::STRING FROM PON_EV_LAB.CURATED.VEHICLE_STATUS WHERE status = 'CHARGING'
UNION ALL
SELECT 'Average Battery %', ROUND(AVG(current_battery), 0)::STRING FROM PON_EV_LAB.CURATED.VEHICLE_STATUS;
