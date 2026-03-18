/*
=============================================================================
PON EV LAB - GOVERNANCE & SHARING DEMO SCRIPT
=============================================================================
Run these queries during the RFP demo to show Resource Monitors and 
Data Sharing in action.
=============================================================================
*/

USE ROLE ACCOUNTADMIN;
USE DATABASE PON_EV_LAB;

-- ============================================================================
-- PART 1: RESOURCE MONITOR (Cost Control)
-- ============================================================================

-- Show the resource monitor attached to our warehouse
SHOW RESOURCE MONITORS LIKE 'PON_LAB_MONITOR';

-- See detailed usage - note the used_credits increasing as we run queries
SELECT 
    name AS monitor_name,
    credit_quota AS monthly_budget,
    used_credits AS credits_used,
    remaining_credits AS credits_remaining,
    ROUND(used_credits / credit_quota * 100, 2) AS pct_used,
    notify_at AS alert_threshold,
    suspend_at AS auto_suspend_threshold
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Show which warehouse is protected
SHOW WAREHOUSES LIKE 'PON_ANALYTICS_WH';

-- See the resource_monitor column - it shows PON_LAB_MONITOR is attached
SELECT 
    "name" AS warehouse,
    "size" AS size,
    "auto_suspend" AS auto_suspend_seconds,
    "resource_monitor" AS cost_control_monitor
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ============================================================================
-- PART 2: SECURE DATA SHARING (Zero-Copy)
-- ============================================================================

-- Show our outbound share
SHOW SHARES LIKE 'PON_DEALER_SHARE';

-- See what's being shared
SHOW GRANTS TO SHARE PON_DEALER_SHARE;

-- Formatted view of shared objects
SELECT 
    privilege,
    granted_on AS object_type,
    name AS object_name,
    granted_to,
    grantee_name AS share_name
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY granted_on, name;

-- ============================================================================
-- PART 3: SIMULATE ADDING A CONSUMER (for demo purposes)
-- ============================================================================

-- In a real scenario, you would add a consumer account like this:
-- ALTER SHARE PON_DEALER_SHARE ADD ACCOUNTS = '<dealer_account_locator>';

-- Show the share is ready for consumers
DESC SHARE PON_DEALER_SHARE;

-- ============================================================================
-- PART 4: QUERY HISTORY & AUDIT (Governance)
-- ============================================================================

-- Show recent queries on our analytics tables (audit trail)
SELECT 
    query_id,
    user_name,
    role_name,
    warehouse_name,
    query_type,
    execution_status,
    total_elapsed_time / 1000 AS seconds,
    query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE database_name = 'PON_EV_LAB'
  AND start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY start_time DESC
LIMIT 10;

-- ============================================================================
-- PART 5: CREDIT CONSUMPTION TRACKING
-- ============================================================================

-- See credit usage by warehouse (last 7 days)
SELECT 
    warehouse_name,
    SUM(credits_used) AS total_credits,
    COUNT(*) AS query_count,
    ROUND(SUM(credits_used) / COUNT(*), 4) AS credits_per_query
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'PON_ANALYTICS_WH'
  AND start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY warehouse_name;

-- ============================================================================
-- DEMO TALKING POINTS
-- ============================================================================
/*
RESOURCE MONITOR:
- "Notice we have automatic cost control - 100 credits/month budget"
- "At 75%, we get an alert. At 100%, the warehouse auto-suspends"
- "No surprise bills - this is built into the platform"
- "Databricks requires custom scripts for this level of control"

DATA SHARING:
- "We're sharing 4 analytics tables with our dealer network"
- "This is ZERO-COPY sharing - dealers query our live data"
- "No ETL, no sync jobs, no stale data"
- "When our Dynamic Tables refresh, dealers see fresh data immediately"
- "Full audit trail - we know exactly who accessed what"

COMPETITIVE ADVANTAGE:
- "Databricks Delta Sharing requires data copying"
- "MS Fabric sharing is limited to Azure ecosystem"
- "Snowflake shares across ANY cloud, ANY region, ZERO copies"
*/
