/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 4: Scaling and Cost Control
=============================================================================
Demonstrates Snowflake's instant scaling and automatic cost guardrails.
=============================================================================
*/

USE DATABASE PON_EV_LAB;

-- =============================================================================
-- Multi-Cluster Warehouse
-- =============================================================================
-- Creates a warehouse that automatically scales for concurrent users.
-- No cluster management, no spin-up wait times.

CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60                -- Suspend after 1 minute idle
    AUTO_RESUME = TRUE               -- Resume instantly on query
    MIN_CLUSTER_COUNT = 1            -- Start with 1 cluster
    MAX_CLUSTER_COUNT = 3            -- Scale to 3 for concurrent users
    SCALING_POLICY = 'STANDARD'      -- Scale based on queue depth
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Multi-cluster warehouse for Pon EV Analytics';

-- =============================================================================
-- Resource Monitor (Cost Control)
-- =============================================================================
-- Prevents runaway spending with automatic alerts and suspension.

CREATE OR REPLACE RESOURCE MONITOR PON_LAB_MONITOR
    WITH CREDIT_QUOTA = 100          -- 100 credits monthly limit
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY      -- Alert at 50% spend
        ON 75 PERCENT DO NOTIFY      -- Alert at 75% spend
        ON 90 PERCENT DO NOTIFY      -- Alert at 90% spend
        ON 100 PERCENT DO SUSPEND    -- Suspend warehouse at limit
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;  -- Hard stop

-- Assign monitor to warehouse
ALTER WAREHOUSE PON_ANALYTICS_WH SET RESOURCE_MONITOR = PON_LAB_MONITOR;

-- =============================================================================
-- Verify Configuration
-- =============================================================================

SHOW WAREHOUSES LIKE 'PON_ANALYTICS_WH';
SHOW RESOURCE MONITORS LIKE 'PON_LAB_MONITOR';

-- =============================================================================
-- Performance Demo
-- =============================================================================
-- Run these queries to demonstrate instant execution

USE WAREHOUSE PON_ANALYTICS_WH;

-- Query 1: Simple count
SELECT 'Count all vehicles' AS query, COUNT(*) AS result 
FROM RAW.VEHICLES_RAW;

-- Query 2: Join performance
SELECT 'Join vehicles + fuel' AS query, COUNT(*) AS result 
FROM CURATED.VEHICLES_WITH_FUEL;

-- Query 3: Aggregation
SELECT 'Aggregation by fuel' AS query, fuel_category, COUNT(*) AS result 
FROM CURATED.VEHICLES_WITH_FUEL
GROUP BY fuel_category
ORDER BY result DESC;

-- Query 4: Complex analytics
SELECT 
    fuel_category,
    registration_year,
    COUNT(*) AS vehicle_count,
    AVG(COUNT(*)) OVER (PARTITION BY fuel_category) AS avg_per_category,
    SUM(COUNT(*)) OVER (PARTITION BY registration_year) AS total_that_year
FROM CURATED.VEHICLES_WITH_FUEL
WHERE registration_year >= 2020
GROUP BY fuel_category, registration_year
ORDER BY registration_year, vehicle_count DESC;

-- =============================================================================
-- Update Dynamic Tables to use our warehouse
-- =============================================================================

ALTER DYNAMIC TABLE CURATED.VEHICLES_WITH_FUEL SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE CURATED.CHARGING_BY_AREA SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE ANALYTICS.EV_GROWTH_TRENDS SET WAREHOUSE = PON_ANALYTICS_WH;
ALTER DYNAMIC TABLE ANALYTICS.EV_YOY_GROWTH SET WAREHOUSE = PON_ANALYTICS_WH;
