<p align="center">
  <img src="assets/banner.svg" alt="Pon Automotive EV Transition Lab" width="100%">
</p>

<h1 align="center">Pon Automotive: EV Transition Lab</h1>

<p align="center">
  <b>Build Scalable Data Pipelines for Electric Vehicle Analytics</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Duration-2_hours-FF6B00?style=for-the-badge" alt="Duration: 2 hours">
  <img src="https://img.shields.io/badge/Level-Intermediate-0069B4?style=for-the-badge" alt="Level: Intermediate">
  <img src="https://img.shields.io/badge/Platform-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Platform: Snowflake">
</p>

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Welcome

In this lab, you'll build a complete data engineering solution to analyze the **Electric Vehicle transition in the Netherlands**. You'll work with real government data from RDW (Dutch Vehicle Authority) and create automated pipelines that answer a key business question:

> *"Which region has the fastest EV growth, and does that correlate with charging infrastructure?"*

### What Makes This Lab Different

This lab focuses on the **data engineering fundamentals** that matter most:

- ✅ **Real API data**: Not synthetic, not CSV uploads, actual live government APIs
- ✅ **Zero orchestration**: Dynamic Tables replace Airflow/Data Factory complexity
- ✅ **Cost control built-in**: Resource monitors prevent runaway spending
- ✅ **Production-ready sharing**: Share live data with partners, no copies

Let's get started.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 0: Environment Setup

**Duration: 10 minutes**

### 0.1 Access Your Snowflake Account

1. Open your browser and navigate to your Snowflake account URL
2. Log in with your credentials
3. Ensure you have **ACCOUNTADMIN** role access (required for this lab)

### 0.2 Verify Your Role

In the top-right corner of Snowsight, click on your username and verify:
- **Role:** ACCOUNTADMIN
- **Warehouse:** Any available warehouse (we'll create a dedicated one)

### 0.3 Open a SQL Workspace

1. Click **Workspaces** in the left navigation
2. Click **+ New Workspace** and name it `Pon EV Lab`
3. Create a new SQL file to start writing queries

You're ready to begin.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 1: Database & Schema Design

**Duration: 10 minutes**

### The Data Architecture

We'll create a **medallion architecture** with three layers:

| Layer | Schema | Purpose |
|-------|--------|---------|
| **Bronze** | RAW | Raw data from APIs, unchanged |
| **Silver** | CURATED | Cleaned, joined, enriched data |
| **Gold** | ANALYTICS | Business-ready aggregations |

### 1.1 Create the Database

```sql
CREATE DATABASE IF NOT EXISTS PON_EV_LAB
    COMMENT = 'Pon Automotive - EV Transition Netherlands Analytics';

USE DATABASE PON_EV_LAB;
```

### 1.2 Create the Schema Layers

```sql
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Raw data from RDW APIs - unchanged source data';

CREATE SCHEMA IF NOT EXISTS CURATED
    COMMENT = 'Curated data - cleaned, validated, and joined';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics layer - aggregations and business metrics';
```

### 1.3 Create Raw Data Tables

```sql
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS VEHICLES_RAW (
    kenteken STRING COMMENT 'License plate number (primary key)',
    datum_eerste_tenaamstelling_in_nederland STRING COMMENT 'First registration date in NL (YYYYMMDD)',
    merk STRING COMMENT 'Vehicle brand (e.g., VOLKSWAGEN, TESLA)',
    handelsbenaming STRING COMMENT 'Commercial model name',
    voertuigsoort STRING COMMENT 'Vehicle type',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

CREATE TABLE IF NOT EXISTS VEHICLES_FUEL_RAW (
    kenteken STRING COMMENT 'License plate number (foreign key)',
    brandstof_omschrijving STRING COMMENT 'Fuel type description',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

CREATE TABLE IF NOT EXISTS PARKING_ADDRESS_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    parkingaddresstype STRING COMMENT 'Type of address (F = facility)',
    zipcode STRING COMMENT 'Postal code',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

CREATE TABLE IF NOT EXISTS CHARGING_CAPACITY_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    chargingpointcapacity STRING COMMENT 'Number of charging points',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);
```

### ✅ Checkpoint

Run this to verify your setup:

```sql
SHOW SCHEMAS IN DATABASE PON_EV_LAB;
SHOW TABLES IN SCHEMA PON_EV_LAB.RAW;
```

You should see 3 schemas and 4 tables.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 2: API Data Ingestion

**Duration: 25 minutes**

This is where Snowflake shines. We'll fetch data directly from external APIs **without any external tools**: no Python scripts on your laptop, no AWS Lambda, no Azure Functions.

### 2.1 Create Network Access Rule

First, we tell Snowflake which external endpoints are allowed:

```sql
CREATE OR REPLACE NETWORK RULE rdw_api_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('opendata.rdw.nl:443')
    COMMENT = 'Allow HTTPS access to RDW Open Data APIs';
```

### 2.2 Create External Access Integration

Now we create an integration that uses this rule:

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION rdw_api_access
    ALLOWED_NETWORK_RULES = (rdw_api_rule)
    ENABLED = TRUE
    COMMENT = 'External access for RDW Open Data API calls';
```

> 💡 **Why This Matters:** With Databricks, you'd need to configure network egress at the cluster level or use a separate service. With Fabric, you'd need Data Factory pipelines. Snowflake makes it declarative and auditable.

### 2.3 Create the API Fetching Function

This Python UDF handles pagination (the RDW API returns data in chunks):

```sql
CREATE OR REPLACE FUNCTION PON_EV_LAB.RAW.FETCH_RDW_DATA(
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
COMMENT = 'Fetches paginated data from RDW Open Data API'
AS $$
import requests

def fetch_data(dataset_id, row_limit, row_offset):
    url = f"https://opendata.rdw.nl/resource/{dataset_id}.json"
    params = {
        "$limit": row_limit,
        "$offset": row_offset
    }
    
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    
    return response.json()
$$;
```

### 2.4 Test the API Connection

Let's verify everything works:

```sql
SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('m9d7-ebf2', 5, 0) AS sample_data;
```

You should see a JSON array with vehicle data.

### 2.5 Load Vehicle Data

Now we load 50,000 vehicles in a single query using `LATERAL FLATTEN`:

```sql
INSERT INTO PON_EV_LAB.RAW.VEHICLES_RAW 
    (kenteken, datum_eerste_tenaamstelling_in_nederland, merk, handelsbenaming, voertuigsoort, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('m9d7-ebf2', 1000, offset_val) AS data
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
```

> 💡 **Performance Note:** This loads 50,000 records in seconds. The `LATERAL FLATTEN` pattern processes all API responses in parallel.

### 2.6 Load Fuel Type Data

```sql
INSERT INTO PON_EV_LAB.RAW.VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 150))
),
api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('8ys7-d773', 1000, offset_val) AS data
    FROM offsets
)
SELECT 
    f.value:kenteken::STRING,
    f.value:brandstof_omschrijving::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;
```

### 2.7 Load Parking and Charging Data

```sql
INSERT INTO PON_EV_LAB.RAW.PARKING_ADDRESS_RAW 
    (areaid, areamanagerid, parkingaddresstype, zipcode, raw_json)
WITH api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('ygq4-hh5q', 10000, 0) AS data
)
SELECT 
    f.value:areaid::STRING,
    f.value:areamanagerid::STRING,
    f.value:parkingaddresstype::STRING,
    f.value:zipcode::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f
WHERE f.value:parkingaddresstype::STRING = 'F';

INSERT INTO PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW 
    (areaid, areamanagerid, chargingpointcapacity, raw_json)
WITH api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('b3us-f26s', 10000, 0) AS data
)
SELECT 
    f.value:areaid::STRING,
    f.value:areamanagerid::STRING,
    f.value:chargingpointcapacity::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;
```

### ✅ Checkpoint

Verify your data load:

```sql
SELECT 'VEHICLES_RAW' AS table_name, COUNT(*) AS row_count FROM PON_EV_LAB.RAW.VEHICLES_RAW
UNION ALL SELECT 'VEHICLES_FUEL_RAW', COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
UNION ALL SELECT 'PARKING_ADDRESS_RAW', COUNT(*) FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW
UNION ALL SELECT 'CHARGING_CAPACITY_RAW', COUNT(*) FROM PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW;
```

Expected: 50K+ vehicles, 150K+ fuel records, 3K+ parking, 3K+ charging.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 3: Dynamic Tables Pipeline

**Duration: 20 minutes**

Here's where Snowflake eliminates pipeline complexity. **Dynamic Tables** are declarative transformations that automatically refresh. No Airflow, no Data Factory, no cron jobs.

### 3.1 Create the Curated Layer

Join vehicles with their fuel types and classify EVs:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Vehicles joined with fuel types, EV classification'
AS
SELECT 
    v.kenteken,
    TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD') AS registration_date,
    YEAR(TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD')) AS registration_year,
    v.merk AS brand,
    v.handelsbenaming AS model,
    v.voertuigsoort AS vehicle_type,
    f.brandstof_omschrijving AS fuel_type,
    
    CASE 
        WHEN f.brandstof_omschrijving ILIKE '%elektr%' THEN 'Electric'
        WHEN f.brandstof_omschrijving ILIKE '%hybride%' THEN 'Hybrid'
        WHEN f.brandstof_omschrijving ILIKE '%waterstof%' THEN 'Hydrogen'
        WHEN f.brandstof_omschrijving ILIKE '%benzine%' THEN 'Petrol'
        WHEN f.brandstof_omschrijving ILIKE '%diesel%' THEN 'Diesel'
        WHEN f.brandstof_omschrijving ILIKE '%lpg%' THEN 'LPG'
        ELSE 'Other'
    END AS fuel_category,
    
    CASE 
        WHEN f.brandstof_omschrijving ILIKE '%elektr%' 
          OR f.brandstof_omschrijving ILIKE '%hybride%'
          OR f.brandstof_omschrijving ILIKE '%waterstof%' 
        THEN TRUE 
        ELSE FALSE 
    END AS is_ev_or_hybrid
    
FROM PON_EV_LAB.RAW.VEHICLES_RAW v
LEFT JOIN PON_EV_LAB.RAW.VEHICLES_FUEL_RAW f 
    ON v.kenteken = f.kenteken;
```

> 💡 **What Just Happened:** This Dynamic Table will automatically refresh within 1 hour of any changes to the source tables. No scheduling, no monitoring, no maintenance.

### 3.2 Create Charging Infrastructure View

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.CHARGING_BY_AREA
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Charging infrastructure aggregated by postal code area'
AS
SELECT 
    p.zipcode,
    LEFT(p.zipcode, 2) AS postal_area,
    COUNT(DISTINCT p.areaid) AS num_parking_locations,
    SUM(TRY_TO_NUMBER(c.chargingpointcapacity)) AS total_charging_points,
    AVG(TRY_TO_NUMBER(c.chargingpointcapacity)) AS avg_charging_per_location
FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW p
LEFT JOIN PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW c 
    ON p.areaid = c.areaid 
    AND p.areamanagerid = c.areamanagerid
WHERE p.zipcode IS NOT NULL
GROUP BY p.zipcode, LEFT(p.zipcode, 2);
```

### 3.3 Create Analytics Layer

Now the business metrics. This answers the key question:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_GROWTH_TRENDS
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'EV registration trends by year'
AS
SELECT 
    registration_year,
    fuel_category,
    COUNT(*) AS vehicle_count,
    SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_hybrid_count,
    ROUND(SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) * 100.0 / 
          NULLIF(COUNT(*), 0), 2) AS ev_percentage
FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
WHERE registration_year IS NOT NULL 
  AND registration_year >= 2015
GROUP BY registration_year, fuel_category
ORDER BY registration_year, fuel_category;

CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Year-over-year EV growth metrics'
AS
WITH yearly_totals AS (
    SELECT 
        registration_year,
        COUNT(*) AS total_vehicles,
        SUM(CASE WHEN is_ev_or_hybrid THEN 1 ELSE 0 END) AS ev_count
    FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
    WHERE registration_year IS NOT NULL 
      AND registration_year >= 2015
    GROUP BY registration_year
)
SELECT 
    registration_year,
    total_vehicles,
    ev_count,
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
```

### 3.4 View Your Pipeline

Check the Dynamic Table status:

```sql
SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;

SELECT 
    name,
    target_lag,
    refresh_mode,
    scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = 'PON_EV_LAB';
```

### ✅ Checkpoint

Query your analytics:

```sql
SELECT * FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH ORDER BY registration_year;
```

You should see EV growth trends over the years.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 4: Scaling & Cost Control

**Duration: 15 minutes**

This module addresses two critical pain points: **slow queries** and **unpredictable costs**.

### 4.1 Create a Multi-Cluster Warehouse

```sql
CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Multi-cluster warehouse for Pon EV Analytics';
```

> 💡 **vs. Databricks:** You'd manually configure autoscaling ranges and wait 2-5 minutes for cluster spin-up.
> 
> 💡 **vs. Fabric:** Capacity units are shared across the workspace. Heavy users affect everyone.

### 4.2 Create a Resource Monitor

Prevent runaway costs with automatic guardrails:

```sql
CREATE OR REPLACE RESOURCE MONITOR PON_LAB_MONITOR
    WITH CREDIT_QUOTA = 100
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE PON_ANALYTICS_WH SET RESOURCE_MONITOR = PON_LAB_MONITOR;
```

### 4.3 Demo: Performance Test

Run this to see instant query execution:

```sql
USE WAREHOUSE PON_ANALYTICS_WH;

SELECT 'Query 1: Count all vehicles' AS test, COUNT(*) AS result 
FROM PON_EV_LAB.RAW.VEHICLES_RAW;

SELECT 'Query 2: Join performance' AS test, COUNT(*) AS result 
FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL;

SELECT 'Query 3: Aggregation' AS test, fuel_category, COUNT(*) AS result 
FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
GROUP BY fuel_category;
```

### 4.4 View Warehouse Status

```sql
SHOW WAREHOUSES LIKE 'PON_ANALYTICS_WH';
SHOW RESOURCE MONITORS LIKE 'PON_LAB_MONITOR';
```

### ✅ Checkpoint

Your warehouse should show:
- State: STARTED or SUSPENDED
- Size: SMALL
- Min/Max Clusters: 1/3

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 5: Secure Data Sharing

**Duration: 15 minutes**

This is Snowflake's **killer feature**: share live data with external organizations without copying data, without ETL pipelines, with instant access control.

### 5.1 Create a Data Share

```sql
CREATE OR REPLACE SHARE PON_DEALER_SHARE
    COMMENT = 'EV analytics data for Pon dealer network - live data, no copies';
```

### 5.2 Grant Access to Objects

```sql
GRANT USAGE ON DATABASE PON_EV_LAB TO SHARE PON_DEALER_SHARE;
GRANT USAGE ON SCHEMA PON_EV_LAB.ANALYTICS TO SHARE PON_DEALER_SHARE;

GRANT SELECT ON TABLE PON_EV_LAB.ANALYTICS.EV_GROWTH_TRENDS TO SHARE PON_DEALER_SHARE;
GRANT SELECT ON TABLE PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH TO SHARE PON_DEALER_SHARE;
```

### 5.3 View the Share

```sql
SHOW SHARES LIKE 'PON_DEALER_SHARE';
SHOW GRANTS TO SHARE PON_DEALER_SHARE;
```

### Key Benefits for Pon

| Benefit | Description |
|---------|-------------|
| **No Data Copies** | Dealers query live data directly |
| **Instant Updates** | When you update data, dealers see it immediately |
| **Revoke Instantly** | Remove access in seconds if needed |
| **Full Audit Trail** | Know exactly who queried what and when |
| **Cross-Cloud** | Works even if dealers are on different cloud providers |

### How Dealers Would Access

When you add a dealer's Snowflake account to this share, they would run:

```sql
CREATE DATABASE PON_EV_DATA FROM SHARE <your_account>.PON_DEALER_SHARE;

SELECT * FROM PON_EV_DATA.ANALYTICS.EV_GROWTH_TRENDS;
```

> 💡 **vs. Databricks:** Delta Sharing requires a separate setup, different protocol, and often involves data copies for cross-cloud scenarios.
> 
> 💡 **vs. Fabric:** No native cross-organization sharing capability.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 6: Streamlit Dashboard

**Duration: 15 minutes**

Build an interactive dashboard directly in Snowflake. No external hosting, no separate deployment.

### 6.1 Create the Streamlit App

1. In Snowsight, go to **Projects** → **Streamlit**
2. Click **+ Streamlit App**
3. Configure:
   - **Name:** `EV_TRANSITION_DASHBOARD`
   - **Warehouse:** `PON_ANALYTICS_WH`
   - **Database/Schema:** `PON_EV_LAB.ANALYTICS`
4. Click **Create**

### 6.2 Add the Dashboard Code

Replace the default code with:

```python
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Pon EV Transition", page_icon="🚗", layout="wide")

session = get_active_session()

st.title("🚗 EV Transition Netherlands")
st.caption("Pon Automotive Data Engineering Lab | Powered by Snowflake")

st.markdown("""
**Business Question:** Which region has the fastest EV growth, 
and does that correlate with charging infrastructure?
""")

tab1, tab2, tab3, tab4 = st.tabs(["📈 EV Growth", "⛽ Fuel Mix", "🔌 Charging Infra", "⚡ Performance"])

with tab1:
    st.subheader("EV Registrations by Year")
    
    df = session.sql("""
        SELECT registration_year, fuel_category, COUNT(*) as count
        FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
        WHERE registration_year >= 2015 AND fuel_type IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1
    """).to_pandas()
    
    if not df.empty:
        pivot = df.pivot(index='REGISTRATION_YEAR', columns='FUEL_CATEGORY', values='COUNT').fillna(0)
        st.bar_chart(pivot)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Vehicles", f"{df['COUNT'].sum():,}")
        with col2:
            ev = df[df['FUEL_CATEGORY'] == 'Electric']['COUNT'].sum()
            st.metric("Electric Vehicles", f"{ev:,}")

with tab2:
    st.subheader("Fuel Type Distribution")
    
    df = session.sql("""
        SELECT brandstof_omschrijving as fuel_type, COUNT(*) as count
        FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
        GROUP BY 1
        ORDER BY 2 DESC
    """).to_pandas()
    
    st.bar_chart(df.set_index('FUEL_TYPE'))

with tab3:
    st.subheader("Charging Infrastructure by Region")
    
    df = session.sql("""
        SELECT postal_area, SUM(total_charging_points) as charging_points
        FROM PON_EV_LAB.CURATED.CHARGING_BY_AREA
        WHERE postal_area IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 15
    """).to_pandas()
    
    if not df.empty:
        st.bar_chart(df.set_index('POSTAL_AREA'))

with tab4:
    st.subheader("⚡ Why Snowflake?")
    
    st.markdown("""
    | Feature | Snowflake | Databricks | Fabric |
    |---------|-----------|------------|--------|
    | **Cluster Management** | None | Manual | Capacity units |
    | **Startup Time** | Instant | 2-5 min | Variable |
    | **Concurrent Users** | Auto-scale | Manual | Shared |
    | **Data Sharing** | Native | Delta Sharing | N/A |
    """)
    
    if st.button("Run Performance Test", type="primary"):
        import time
        results = []
        
        for name, sql in [
            ("Count vehicles", "SELECT COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_RAW"),
            ("Join tables", "SELECT COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL"),
            ("Aggregate data", "SELECT fuel_category, COUNT(*) FROM PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL GROUP BY 1"),
        ]:
            start = time.time()
            session.sql(sql).collect()
            ms = round((time.time() - start) * 1000, 1)
            results.append({"Query": name, "Time (ms)": ms})
        
        st.dataframe(pd.DataFrame(results), hide_index=True)
        st.success("No cluster warmup needed!")

st.divider()
st.caption("Database: PON_EV_LAB | Warehouse: PON_ANALYTICS_WH | Share: PON_DEALER_SHARE")
```

### 6.3 Run the App

Click **Run** in the top-right corner. Your dashboard is now live.

### ✅ Checkpoint

You should see an interactive dashboard with:
- EV growth chart
- Fuel type distribution
- Charging infrastructure map
- Performance comparison table

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 7: Wrap-up & Discussion

**Duration: 10 minutes**

### What We Built

| Component | Snowflake Feature | Benefit |
|-----------|-------------------|---------|
| API Ingestion | External Access + UDFs | No external tools needed |
| Data Pipeline | Dynamic Tables | Zero orchestration |
| Scaling | Multi-cluster Warehouse | Instant, automatic |
| Cost Control | Resource Monitors | Predictable spend |
| Data Sharing | Secure Shares | Live data, no copies |
| Dashboard | Streamlit in Snowflake | No separate hosting |

### Key Differentiators

**vs. Databricks:**
- No cluster management or spin-up time
- Native sharing without Delta Sharing setup
- Streamlit built-in vs. separate deployment

**vs. Microsoft Fabric:**
- No capacity unit complexity
- No shared pool throttling
- True cross-organization sharing

### Resources Created

```
PON_EV_LAB (Database)
├── RAW (Schema)
│   ├── VEHICLES_RAW
│   ├── VEHICLES_FUEL_RAW
│   ├── PARKING_ADDRESS_RAW
│   └── CHARGING_CAPACITY_RAW
├── CURATED (Schema)
│   ├── VEHICLES_WITH_FUEL (Dynamic Table)
│   └── CHARGING_BY_AREA (Dynamic Table)
└── ANALYTICS (Schema)
    ├── EV_GROWTH_TRENDS (Dynamic Table)
    ├── EV_YOY_GROWTH (Dynamic Table)
    └── EV_TRANSITION_DASHBOARD (Streamlit)

PON_ANALYTICS_WH (Warehouse)
PON_LAB_MONITOR (Resource Monitor)
PON_DEALER_SHARE (Data Share)
```

### Next Steps

1. **Load more data**: Increase API offsets to get fuller dataset
2. **Add regional analysis**: Join with postal code regions
3. **Schedule refreshes**: Adjust Dynamic Table lag for production
4. **Add dealers to share**: Provide real dealer Snowflake accounts

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Bonus Module: Marketplace Data Enrichment

**Duration: 10 minutes** | **Optional**

> This module demonstrates how Snowflake Marketplace can enrich your analysis with third-party data. This is outside the core use case scope but showcases a key platform differentiator.

### Why Marketplace Matters

Unlike Databricks or Fabric, Snowflake offers a **native data marketplace** with 2,500+ free and paid datasets. No ETL, no data movement, instant access.

**Relevant datasets for EV analysis:**
- Weather data (EV range varies by temperature)
- Demographics (income, urbanization correlate with EV adoption)
- Energy prices (charging costs impact ownership decisions)
- CBS statistics (official Netherlands statistics)

### Step 1: Browse Marketplace

1. Navigate to **Data Products** → **Marketplace**
2. Search for `netherlands` or `weather`
3. Filter by **Free** datasets
4. Look for datasets like:
   - Knoema demographic data
   - Open weather historical data
   - CBS Netherlands statistics

### Step 2: Get a Dataset

For this example, we'll use a free weather dataset (actual availability may vary):

1. Click **Get** on a weather dataset
2. Accept the terms
3. The data appears instantly in your account under **Data** → **Shared with Me**

### Step 3: Query Combined Data

Once you have access to a weather dataset (example structure):

```sql
-- Example: Correlate EV growth with average temperature by province
-- Adjust table/column names based on actual Marketplace dataset

SELECT 
    e.PROVINCE,
    e.YEAR,
    e.YOY_GROWTH_PCT,
    w.AVG_TEMPERATURE_C,
    w.HEATING_DEGREE_DAYS
FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH e
LEFT JOIN WEATHER_DB.PUBLIC.NL_PROVINCE_WEATHER w
    ON e.PROVINCE = w.PROVINCE
    AND e.YEAR = w.YEAR
ORDER BY e.YOY_GROWTH_PCT DESC;
```

### Step 4: Add to Streamlit

You can extend the dashboard with a correlation analysis tab:

```python
# Add to streamlit_app.py - Marketplace Enrichment tab
weather_tab = st.tabs(["Weather Correlation"])

with weather_tab:
    st.subheader("EV Adoption vs Temperature")
    
    # Scatter plot: temperature vs EV adoption rate
    correlation_data = session.sql("""
        SELECT PROVINCE, AVG_TEMP, EV_GROWTH_PCT 
        FROM enriched_view
    """).to_pandas()
    
    st.scatter_chart(correlation_data, x="AVG_TEMP", y="EV_GROWTH_PCT")
```

### Key Differentiator

| Platform | Third-party Data | Integration Effort |
|----------|------------------|-------------------|
| **Snowflake** | 2,500+ datasets, instant access | Zero ETL |
| Databricks | Delta Sharing (limited catalog) | Partner setup required |
| Fabric | OneLake shortcuts (Microsoft ecosystem only) | Configuration needed |

### Marketplace Value for Pon

- **Dealer enrichment**: Add demographic data per region
- **Demand forecasting**: Weather patterns affect EV range/charging
- **Competitive analysis**: Combine with market share data
- **Sustainability reporting**: Carbon intensity by energy source

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Appendix: Cleanup

If you want to remove all lab resources:

```sql
DROP DATABASE IF EXISTS PON_EV_LAB;
DROP WAREHOUSE IF EXISTS PON_ANALYTICS_WH;
DROP RESOURCE MONITOR IF EXISTS PON_LAB_MONITOR;
DROP SHARE IF EXISTS PON_DEALER_SHARE;
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS rdw_api_access;
DROP NETWORK RULE IF EXISTS rdw_api_rule;
```

---

<p align="center">
  <img src="https://img.shields.io/badge/Built_for-Pon_Automotive-FF6B00?style=flat-square" alt="Built for Pon Automotive">
  <img src="https://img.shields.io/badge/Powered_by-Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white" alt="Powered by Snowflake">
</p>
