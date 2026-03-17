# Facilitator Setup Checklist

Pre-lab preparation for workshop facilitators.

## One Week Before

- [ ] Confirm participant list (4-5 people)
- [ ] Verify participants have Snowflake trial accounts ($400 credit)
- [ ] Test RDW API endpoints are accessible
- [ ] Review lab guide for any updates needed

## Day Before

- [ ] Run complete setup script (`scripts/00_complete_setup.sql`) on demo account
- [ ] Verify all Dynamic Tables have refreshed with data
- [ ] Test Streamlit dashboard is working
- [ ] Prepare backup CSV files (in case API is slow)

## Day Of

### Environment Checklist
- [ ] Snowflake accounts are accessible
- [ ] ACCOUNTADMIN role available to all participants
- [ ] Internet connectivity for API calls
- [ ] Projected screen for live demos

### Data Verification
Run these queries to verify lab readiness:

```sql
-- Check data volumes
SELECT 'VEHICLES_RAW' AS tbl, COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_RAW
UNION ALL SELECT 'VEHICLES_FUEL_RAW', COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
UNION ALL SELECT 'PARKING_ADDRESS_RAW', COUNT(*) FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW
UNION ALL SELECT 'CHARGING_CAPACITY_RAW', COUNT(*) FROM PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW;

-- Check Dynamic Tables
SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;

-- Verify EV data exists
SELECT fuel_category, COUNT(*) 
FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL 
WHERE fuel_type IS NOT NULL
GROUP BY 1;
```

## Key Talking Points

### Module 2: API Ingestion
- **Emphasize:** No external tools needed (vs. Databricks/Fabric)
- **Demo:** Show the UDF calling the API in real-time
- **Compare:** "With Databricks, you'd need a cluster + notebook + external library"

### Module 3: Dynamic Tables
- **Emphasize:** Zero orchestration (vs. Airflow/Data Factory)
- **Demo:** Show `TARGET_LAG` and explain automatic refresh
- **Compare:** "With Fabric, you'd need Data Factory pipelines for this"

### Module 4: Scaling
- **Emphasize:** Instant scaling, no warmup
- **Demo:** Run concurrent queries, show multi-cluster activation
- **Compare:** "Databricks clusters take 2-5 minutes to spin up"

### Module 5: Data Sharing
- **THIS IS THE KILLER FEATURE**
- **Emphasize:** Zero-copy, live data, instant revocation
- **Demo:** Show `SHOW GRANTS TO SHARE` 
- **Compare:** "Databricks requires Delta Sharing setup. Fabric can't do this."

## Troubleshooting

### API Timeout
If RDW API is slow, reduce batch sizes:
```sql
-- Change from 50 batches to 10
FROM TABLE(GENERATOR(ROWCOUNT => 10))
```

### Low EV Match Rate
The paginated API returns different vehicles each time. If EV count is too low:
```sql
-- Load more fuel data with different offsets
INSERT INTO VEHICLES_FUEL_RAW ...
FROM TABLE(GENERATOR(ROWCOUNT => 200))  -- Increase to 200K records
```

### Dynamic Table Not Refreshing
Force manual refresh:
```sql
ALTER DYNAMIC TABLE PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL REFRESH;
```

## Post-Lab

- [ ] Collect feedback from participants
- [ ] Note any issues encountered
- [ ] Update lab guide if needed
- [ ] Share trial account extension process (3-month POC)
