/*
=============================================================================
PON EV LAB - READER ACCOUNT SETUP (Optional - for full sharing demo)
=============================================================================
Creates a managed reader account to demonstrate data sharing end-to-end.
This allows you to show the consumer experience during the RFP demo.
=============================================================================
*/

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- OPTION 1: CREATE A READER ACCOUNT (Recommended for demos)
-- ============================================================================

-- Create a managed reader account for the "dealer"
CREATE MANAGED ACCOUNT PON_DEALER_DEMO
    ADMIN_NAME = 'dealer_admin',
    ADMIN_PASSWORD = 'DemoPassword123!',
    TYPE = READER,
    COMMENT = 'Demo reader account for Pon dealer data sharing';

-- Get the reader account locator
SHOW MANAGED ACCOUNTS LIKE 'PON_DEALER_DEMO';

-- Add the reader account to our share (replace <locator> with actual value)
-- ALTER SHARE PON_DEALER_SHARE ADD ACCOUNTS = '<locator>';

-- ============================================================================
-- OPTION 2: USE AN EXISTING SNOWFLAKE ACCOUNT
-- ============================================================================

-- If you have another Snowflake account (e.g., a sandbox), you can share to it:
-- ALTER SHARE PON_DEALER_SHARE ADD ACCOUNTS = 'ORG_NAME.ACCOUNT_NAME';

-- List current accounts the share is available to:
SHOW GRANTS OF SHARE PON_DEALER_SHARE;

-- ============================================================================
-- ON THE CONSUMER SIDE (run in the reader/consumer account)
-- ============================================================================

/*
-- 1. See available shares
SHOW SHARES;

-- 2. Create a database from the share
CREATE DATABASE PON_DEALER_DATA FROM SHARE <provider_account>.PON_DEALER_SHARE;

-- 3. Query the shared data (zero-copy, always live!)
USE DATABASE PON_DEALER_DATA;
SELECT * FROM ANALYTICS.EV_GROWTH_TRENDS LIMIT 10;
SELECT * FROM ANALYTICS.EV_YOY_GROWTH LIMIT 10;
SELECT * FROM ANALYTICS.EV_INFRASTRUCTURE_CORRELATION LIMIT 10;

-- 4. Notice: No data was copied! This queries the provider's live data.
*/

-- ============================================================================
-- CLEANUP (after demo)
-- ============================================================================

-- DROP MANAGED ACCOUNT PON_DEALER_DEMO;
