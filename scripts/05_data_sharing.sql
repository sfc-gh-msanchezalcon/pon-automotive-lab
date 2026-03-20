/*
=============================================================================
PON AUTOMOTIVE - EV TRANSITION NETHERLANDS
Module 5: Secure Data Sharing
=============================================================================
Snowflake's killer feature: Share live data across organizations without
copying data, without ETL pipelines, with instant access control.
=============================================================================
*/

USE DATABASE PON_EV_LAB;

-- =============================================================================
-- Create Data Share for Dealer Network
-- =============================================================================

CREATE OR REPLACE SHARE PON_DEALER_SHARE
    COMMENT = 'EV analytics for Pon dealer network - live data, no copies';

-- =============================================================================
-- Grant Access to Database and Schema
-- =============================================================================

GRANT USAGE ON DATABASE PON_EV_LAB TO SHARE PON_DEALER_SHARE;
GRANT USAGE ON SCHEMA ANALYTICS TO SHARE PON_DEALER_SHARE;

-- =============================================================================
-- Share Analytics Tables (Read-Only)
-- =============================================================================

-- Share EV growth trends
GRANT SELECT ON TABLE ANALYTICS.EV_GROWTH_TRENDS TO SHARE PON_DEALER_SHARE;

-- Share year-over-year metrics
GRANT SELECT ON TABLE ANALYTICS.EV_YOY_GROWTH TO SHARE PON_DEALER_SHARE;

-- Share infrastructure correlation
GRANT SELECT ON TABLE ANALYTICS.EV_INFRASTRUCTURE_CORRELATION TO SHARE PON_DEALER_SHARE;

-- =============================================================================
-- Verify Share Configuration
-- =============================================================================

SHOW SHARES LIKE 'PON_DEALER_SHARE';
SHOW GRANTS TO SHARE PON_DEALER_SHARE;

-- =============================================================================
-- How Dealers Would Access (for reference)
-- =============================================================================
/*
-- On dealer's Snowflake account, they would run:

-- Create database from share
CREATE DATABASE PON_DEALER_DATA FROM SHARE <pon_account>.PON_DEALER_SHARE;

-- Query live data (no copies!)
SELECT * FROM PON_DEALER_DATA.ANALYTICS.EV_GROWTH_TRENDS;
SELECT * FROM PON_DEALER_DATA.ANALYTICS.EV_YOY_GROWTH;

-- They always see the latest data - automatic sync
*/

-- =============================================================================
-- Additional Share for OEM Partners (conceptual - see lab guide Section 5.6)
-- =============================================================================
-- In production, Pon would create separate shares per partner type:
-- CREATE OR REPLACE SHARE PON_OEM_SHARE
--     COMMENT = 'EV market data for OEM partners (anonymized)';
-- GRANT USAGE ON DATABASE PON_EV_LAB TO SHARE PON_OEM_SHARE;
-- GRANT USAGE ON SCHEMA ANALYTICS TO SHARE PON_OEM_SHARE;
-- GRANT SELECT ON TABLE ANALYTICS.EV_GROWTH_TRENDS TO SHARE PON_OEM_SHARE;

-- =============================================================================
-- Key Benefits Summary
-- =============================================================================
/*
1. NO DATA COPIES
   - Dealers query live data directly from your account
   - No storage duplication, no sync delays
   
2. INSTANT ACCESS CONTROL
   - Revoke access in seconds: DROP SHARE PON_DEALER_SHARE;
   - Add new tables instantly: GRANT SELECT ON TABLE ... TO SHARE ...;
   
3. FULL AUDIT TRAIL
   - Account Usage tracks who queried what
   - Complete visibility into data access patterns
   
4. CROSS-CLOUD SUPPORT
   - Works even if dealers are on different cloud providers
   - Snowflake handles replication transparently
   
5. GOVERNANCE
   - Row-level security can be applied
   - Column masking for sensitive data
   - Data stays in your account
*/
