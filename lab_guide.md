<p align="center">
  <img src="assets/banner.svg" alt="Pon EV Intelligence Lab" width="100%">
</p>

<h1 align="center">Pon EV Intelligence</h1>

<p align="center">
  <b>Data Engineering for Strategic EV Planning</b>
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

- Real API data: Not synthetic, not CSV uploads, actual live government APIs
- Zero orchestration: Dynamic Tables eliminate the need for external schedulers
- Cost control built-in: Resource monitors prevent runaway spending
- Production-ready sharing: Share live data with partners, no copies

### The Journey

| Step | Module | What You'll Do |
|------|--------|----------------|
| 1 | **Ingest** | Pull live data from government APIs |
| 2 | **Transform** | Build automated pipelines with Dynamic Tables |
| 3 | **Control** | Set cost guardrails and scaling policies |
| 4 | **Share** | Publish live data to partners |
| 5 | **Enrich** | Add third-party data from the Marketplace |
| 6 | **Visualize** | Build an interactive Streamlit dashboard |

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

> **Why This Matters for Pon**
> 
> Pon currently struggles with **data silos** - vehicle registration data, fuel type data, and charging infrastructure data live in separate systems with no unified view. Leadership cannot answer basic questions like "Which region has the fastest EV growth?" without manual data gathering from multiple sources.
> 
> The **medallion architecture** (RAW -> CURATED -> ANALYTICS) creates a single source of truth that leadership can trust for strategic decisions about EV inventory allocation and charging infrastructure partnerships. This directly addresses the PDF use case objective.

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

-- Individual vehicle registrations (for time-series analysis)
CREATE TABLE IF NOT EXISTS VEHICLES_RAW (
    kenteken STRING COMMENT 'License plate number (primary key)',
    datum_eerste_tenaamstelling_in_nederland STRING COMMENT 'First registration date (YYYYMMDD)',
    merk STRING COMMENT 'Vehicle brand (e.g., VOLKSWAGEN, TESLA)',
    handelsbenaming STRING COMMENT 'Commercial model name',
    voertuigsoort STRING COMMENT 'Vehicle type',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Vehicles aggregated by postal code and fuel type (regional snapshot)
CREATE TABLE IF NOT EXISTS VEHICLES_BY_POSTCODE_RAW (
    postcode STRING COMMENT '4-digit postal code',
    voertuigsoort STRING COMMENT 'Vehicle type (Personenauto = passenger car)',
    brandstof STRING COMMENT 'Fuel type: B=Benzine, D=Diesel, E=Electric',
    extern_oplaadbaar STRING COMMENT 'Plug-in capable: J=Yes, N=No',
    aantal INT COMMENT 'Number of vehicles',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Fuel types per individual vehicle
CREATE TABLE IF NOT EXISTS VEHICLES_FUEL_RAW (
    kenteken STRING COMMENT 'License plate number',
    brandstof_omschrijving STRING COMMENT 'Fuel type description',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Parking locations
CREATE TABLE IF NOT EXISTS PARKING_ADDRESS_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    parkingaddresstype STRING COMMENT 'Type of address (F = facility)',
    zipcode STRING COMMENT 'Postal code',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);

-- Charging infrastructure
CREATE TABLE IF NOT EXISTS CHARGING_CAPACITY_RAW (
    areaid STRING COMMENT 'Parking area identifier',
    areamanagerid STRING COMMENT 'Area manager identifier',
    chargingpointcapacity STRING COMMENT 'Number of charging points',
    raw_json VARIANT COMMENT 'Complete JSON record from API'
);
```

> **What is VARIANT?** A Snowflake data type that stores semi-structured data (JSON, Avro, Parquet). We store the raw API response here for auditability.

### Checkpoint

Run this to verify your setup:

```sql
SHOW SCHEMAS IN DATABASE PON_EV_LAB;
SHOW TABLES IN SCHEMA PON_EV_LAB.RAW;
```

You should see 3 schemas and 5 tables.

> **Stop and notice:** How much cluster configuration have we done so far? None. The compute is serverless — it just works.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 2: API Data Ingestion

**Duration: 25 minutes**

> **Why This Matters for Pon**
> 
> The PDF use case specifies **5 RDW datasets** as the authoritative source for Dutch vehicle registrations. Today, getting this data requires manual CSV exports, FTP transfers, and error-prone processes that delay insights by days.
> 
> By pulling directly from RDW APIs into Snowflake, Pon gets **real-time government data** without intermediaries. The specific columns we extract (kenteken, brandstof_omschrijving, parkingaddressreference, chargingpointcapacity) are exactly what the PDF requires to answer: *"Welke regio heeft de snelste groei van EV's?"*

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

> **Why This Matters:** Other platforms require configuring network egress at the cluster/workspace level or using separate ingestion services. Snowflake's External Access Integration is declarative, auditable, and requires no additional infrastructure.

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

> **What is LATERAL FLATTEN?** A Snowflake function that expands a JSON array into rows. Each element becomes a separate row we can query with standard SQL.

### 2.5 Load Vehicles by Postal Code (KEY Dataset)

This is the most important dataset — it answers our business question about regional EV adoption:

```sql
INSERT INTO PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW 
    (postcode, voertuigsoort, brandstof, extern_oplaadbaar, aantal, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('8wbe-pu7d', 1000, offset_val) AS data
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
```

> **What is GENERATOR?** Creates a virtual table with the specified number of rows. Combined with ROW_NUMBER, we generate offset values (0, 1000, 2000...) for API pagination.

### 2.6 Load Vehicle Registrations (for time-series)

This dataset has registration dates, enabling us to analyze EV growth over time:

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

### 2.7 Load Fuel Type Data

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

### 2.8 Load Parking and Charging Data

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

### Checkpoint

Verify your data load:

```sql
SELECT 'VEHICLES_RAW' AS table_name, COUNT(*) AS row_count FROM PON_EV_LAB.RAW.VEHICLES_RAW
UNION ALL SELECT 'VEHICLES_BY_POSTCODE_RAW', COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
UNION ALL SELECT 'VEHICLES_FUEL_RAW', COUNT(*) FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW
UNION ALL SELECT 'PARKING_ADDRESS_RAW', COUNT(*) FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW
UNION ALL SELECT 'CHARGING_CAPACITY_RAW', COUNT(*) FROM PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW;
```

Expected: ~50K vehicle registrations, ~47K vehicles by postcode, 150K fuel records, 3K parking, 3K charging.

### Scaling for Production (Facilitator Note)

This lab uses controlled data volumes for a 2-hour workshop. For a pre-seeded demo environment with millions of rows:

| Dataset | Lab Setting | Production Setting |
|---------|-------------|-------------------|
| Vehicles | `ROWCOUNT => 50` (50K) | `ROWCOUNT => 500` (500K+) |
| Fuel Types | `ROWCOUNT => 150` (150K) | `ROWCOUNT => 1000` (1M+) |

To demonstrate the "6-hour queries to seconds" story:
1. Pre-load millions of rows in a demo account
2. Use a MEDIUM or LARGE warehouse for the performance test
3. Show query history comparing execution times

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 3: Dynamic Tables Pipeline

**Duration: 20 minutes**

> **Why This Matters for Pon**
> 
> The PDF defines two **target data models** (DOEL) that Pon needs for strategic planning:
> - **brandst per postcode per datum**: Tracks fuel type registrations over time by region - essential for understanding EV adoption velocity
> - **Laadpalen per postcode**: Maps charging infrastructure to postal codes - essential for identifying infrastructure gaps
> 
> Today, creating these models requires overnight batch jobs that are often stale by morning. **Dynamic Tables** automatically keep these models fresh - no schedulers, no monitoring dashboards, no 3am alerts. When RDW updates their data, Pon's analytics update automatically.

Here's where Snowflake eliminates pipeline complexity. **Dynamic Tables** are declarative transformations that automatically refresh — no external orchestration required.

> **💡 Talking Point:** With Dynamic Tables, there's no separate scheduler to configure or monitor. Snowflake handles the refresh logic, dependencies, and incremental updates automatically. You just write SQL.

> **What is TARGET_LAG?** The maximum time Snowflake allows data to be stale before automatically refreshing. `TARGET_LAG = '1 hour'` means data is never more than 1 hour old.

First, create the warehouse that Dynamic Tables will use for refresh:

```sql
CREATE OR REPLACE WAREHOUSE PON_ANALYTICS_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Pon EV Analytics';
```

> **Watch the bottom-right corner** when you run this. The warehouse is ready instantly — no cluster provisioning, no warmup time.

### 3.1 Create the KEY Dynamic Table: EV by Region

This directly answers our business question — EV adoption by postal area:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.EV_BY_REGION
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'EV adoption metrics by postal area'
AS
SELECT 
    LEFT(postcode, 2) AS postal_area,
    SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) AS electric_vehicles,
    SUM(CASE WHEN brandstof = 'B' THEN aantal ELSE 0 END) AS petrol_vehicles,
    SUM(CASE WHEN brandstof = 'D' THEN aantal ELSE 0 END) AS diesel_vehicles,
    SUM(aantal) AS total_vehicles,
    ROUND(100.0 * SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) / NULLIF(SUM(aantal), 0), 2) AS ev_percentage
FROM PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
WHERE voertuigsoort = 'Personenauto'
GROUP BY LEFT(postcode, 2);
```

### 3.2 Create Charging Infrastructure Table

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.CHARGING_BY_AREA
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
AS
SELECT 
    areamanagerid AS area_manager_id,
    COUNT(*) AS num_parking_areas,
    SUM(TRY_CAST(chargingpointcapacity AS INT)) AS total_charging_points
FROM PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW
GROUP BY areamanagerid;
```

### 3.3 Create the Correlation Analysis

This joins EV data with infrastructure to answer: "Does charging availability correlate with EV adoption?"

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
AS
SELECT 
    e.postal_area,
    e.electric_vehicles,
    e.ev_percentage,
    COALESCE(p.parking_locations, 0) AS parking_locations,
    CASE 
        WHEN p.parking_locations > 0 
        THEN ROUND(e.electric_vehicles / p.parking_locations, 0) 
    END AS evs_per_parking_location
FROM PON_EV_LAB.CURATED.EV_BY_REGION e
LEFT JOIN (
    SELECT LEFT(zipcode, 2) AS postal_area, COUNT(*) AS parking_locations
    FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW 
    WHERE zipcode IS NOT NULL AND zipcode != ''
    GROUP BY LEFT(zipcode, 2)
) p ON e.postal_area = p.postal_area
WHERE e.total_vehicles > 5000;
```

### 3.4 Create the Target Model: Laadpalen per Postcode

This is a key target model from the PDF requirements - charging points by postal code:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.LAADPALEN_PER_POSTCODE
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Charging points per postal code - joins Parkeeradres with SPECIFICATIES'
AS
SELECT 
    LEFT(p.zipcode, 4) AS postcode,
    SUM(TRY_CAST(c.chargingpointcapacity AS INT)) AS aantal
FROM PON_EV_LAB.RAW.PARKING_ADDRESS_RAW p
JOIN PON_EV_LAB.RAW.CHARGING_CAPACITY_RAW c 
    ON p.RAW_JSON:parkingaddressreference::STRING = c.areamanagerid
WHERE p.parkingaddresstype = 'F'
  AND p.zipcode IS NOT NULL 
  AND p.zipcode != ''
  AND TRY_CAST(c.chargingpointcapacity AS INT) > 0
GROUP BY LEFT(p.zipcode, 4);
```

> **Data Model Note:** The join key is `parkingaddressreference` (from Parkeeradres) = `areamanagerid` (from SPECIFICATIES PARKEERGEBIED), NOT `areaid`. This links parking locations to their charging capacity.

### 3.5 View Your Pipeline

Check the Dynamic Table status:

```sql
SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;
```

### 3.6 Query the Results

```sql
-- Top regions by EV adoption
SELECT * FROM PON_EV_LAB.CURATED.EV_BY_REGION 
ORDER BY ev_percentage DESC LIMIT 10;

-- Infrastructure correlation
SELECT * FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
ORDER BY evs_per_parking_location DESC LIMIT 10;

-- Charging points by postal code (target model from PDF)
SELECT * FROM PON_EV_LAB.ANALYTICS.LAADPALEN_PER_POSTCODE
ORDER BY aantal DESC LIMIT 10;
```

### Checkpoint

You should see:
- 4+ Dynamic Tables in CURATED and ANALYTICS schemas
- EV_BY_REGION showing ~90 postal areas with EV percentages
- EV_INFRASTRUCTURE_CORRELATION showing EVs per parking location
- LAADPALEN_PER_POSTCODE showing charging points by 4-digit postal code

> **Stop and count:** How many DAG definitions did we write? How many trigger configurations? How many monitoring dashboards did we set up? Zero. The pipeline just works.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 4: Scaling & Cost Control

**Duration: 15 minutes**

> **Why This Matters for Pon**
> 
> Pon's current analytics platform suffers from **6-hour query times** during peak periods when multiple dealers and analysts run reports simultaneously. Users get frustrated, sessions timeout, and business decisions get delayed.
> 
> **Multi-cluster warehouses** ensure that when 50 dealers query simultaneously during a Monday morning sales meeting, no one waits - Snowflake automatically scales out. **Resource monitors** prevent unexpected cloud bills - a critical concern when migrating from predictable on-premise infrastructure to cloud consumption models.

Now let's enhance our warehouse with scaling and cost controls.

### 4.1 Enable Multi-Cluster Scaling

Upgrade the warehouse to handle concurrent users:

```sql
ALTER WAREHOUSE PON_ANALYTICS_WH SET
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD';
```

> **What is Multi-Cluster?** When multiple users run queries simultaneously, Snowflake automatically spins up additional clusters. Each user gets dedicated compute — no one waits.

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

> **This is a hard limit, not a dashboard.** When you hit 100%, the warehouse suspends. No surprises at end-of-month billing.

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

### Checkpoint

Your warehouse should show:
- State: STARTED or SUSPENDED
- Size: SMALL
- Min/Max Clusters: 1/3

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 5: Secure Data Sharing

**Duration: 15 minutes**

> **Why This Matters for Pon**
> 
> Pon's dealer network needs access to regional EV adoption data for inventory planning - which models to stock, where to invest in charging partnerships. Today, this requires **manual CSV exports** emailed to dealers, creating ungoverned copies that quickly become stale and inconsistent.
> 
> **Secure Data Sharing** gives dealers live, governed access to curated analytics. When Pon updates their data, dealers see it immediately. When a dealer relationship ends, access is revoked in seconds - no chasing down spreadsheet copies. This transforms data from a liability into a strategic asset for the dealer network.

This is Snowflake's **killer feature**: share live data with external organizations without copying data, without ETL pipelines, with instant access control.

> **💡 Talking Point:** This is zero-copy sharing — dealers query your live data directly. No exports, no transfers, no stale copies. When you update the data, they see it immediately. And it works across any cloud provider.

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

### 5.3 Verify the Share Contents

```sql
SHOW SHARES LIKE 'PON_DEALER_SHARE';

DESCRIBE SHARE PON_DEALER_SHARE;
```

You should see:
- `kind`: OUTBOUND
- `database_name`: PON_EV_LAB
- Objects: EV_GROWTH_TRENDS, EV_YOY_GROWTH

### 5.4 View in Snowsight UI

To see your share visually:

1. Navigate to **Data Products** > **Private Sharing**
2. Click the **Shared by My Account** tab
3. You should see `PON_DEALER_SHARE` listed
4. Click on it to see the shared objects and manage consumers

### 5.5 Consumer Access (Optional Demo)

If you have access to a second Snowflake account, you can test the full flow:

**On the Provider (your account):**
```sql
-- Add the consumer account (replace with actual account locator)
ALTER SHARE PON_DEALER_SHARE ADD ACCOUNTS = '<consumer_account_locator>';

-- Verify the consumer was added
SHOW GRANTS OF SHARE PON_DEALER_SHARE;
```

**On the Consumer (second account):**
```sql
-- See available shares
SHOW SHARES;

-- Create a database from the share
CREATE DATABASE PON_EV_DATA FROM SHARE <provider_account>.PON_DEALER_SHARE;

-- Query the shared data (live, no copy!)
SELECT * FROM PON_EV_DATA.ANALYTICS.EV_GROWTH_TRENDS LIMIT 10;
SELECT * FROM PON_EV_DATA.ANALYTICS.EV_YOY_GROWTH;
```

### Key Benefits for Pon

| Benefit | Description |
|---------|-------------|
| **No Data Copies** | Dealers query live data directly |
| **Instant Updates** | When you update data, dealers see it immediately |
| **Revoke Instantly** | Remove access in seconds if needed |
| **Full Audit Trail** | Know exactly who queried what and when |
| **Cross-Cloud** | Works even if dealers are on different cloud providers |

> **Snowflake Advantage:** Secure Data Sharing works across any cloud, any region, with no data movement. Recipients query live data directly — always current, never stale.

### 5.6 Production Governance (Conceptual)

In production, Pon would likely need different dealers to see different data. Snowflake supports this with:

**Row Access Policies** — Filter data by dealer identity:
```sql
-- Example: Each dealer sees only their region's data
CREATE ROW ACCESS POLICY dealer_region_policy AS (region STRING)
RETURNS BOOLEAN -> region IN (SELECT allowed_region FROM dealer_permissions WHERE dealer_id = CURRENT_ROLE());
```

**Masking Policies** — Hide sensitive columns:
```sql
-- Example: Mask license plates for non-admin users
CREATE MASKING POLICY mask_kenteken AS (val STRING)
RETURNS STRING -> CASE WHEN IS_ROLE_IN_SESSION('ADMIN') THEN val ELSE '****' END;
```

**Separate Shares per Dealer Group** — Different data views for different partners:
- `PON_DEALER_SHARE_NORTH` — Northern region dealers
- `PON_DEALER_SHARE_SOUTH` — Southern region dealers
- `PON_OEM_SHARE` — Manufacturer partners (aggregated only)

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 6: Marketplace Data Enrichment

**Duration: 15 minutes**

> **Why This Matters for Pon**
> 
> To truly answer *"does EV growth correlate with charging infrastructure?"*, Pon needs external context: **weather data** (cold weather reduces EV range, affecting buyer confidence), **demographic data** (income levels affect EV purchasing power), and **energy prices** (electricity costs impact total cost of ownership vs. petrol).
> 
> Acquiring and integrating this data traditionally requires procurement, contracts, ETL pipelines, and ongoing maintenance. **Snowflake Marketplace** provides instant access to 2,500+ datasets that appear in your account immediately - no ETL, no storage costs for shared data, no ongoing data pipeline maintenance.

In the previous module, you shared YOUR data with partners. Now let's do the reverse: **consume external data** to enrich your analysis. Together, Data Sharing (data out) and Marketplace (data in) represent Snowflake's complete data collaboration story.

### Why Marketplace Matters

Snowflake Marketplace offers **2,500+ datasets** that appear instantly in your account — no ETL, no data movement, no storage costs for shared data.

### 6.1 Get the Datasets

We'll use two free datasets that everyone can access:

**Dataset 1: Weather Data**
1. Navigate to **Data Products** > **Marketplace**
2. Search for: `Global Weather & Climate Data for BI`
3. Select the listing from **Pelmorex Weather Source**
4. Click **Get** and accept the terms
5. Database name: `WEATHER_SOURCE` (or use the suggested name)

**Dataset 2: Economic & Demographic Data**
1. Search for: `Snowflake Public Data (Free)`
2. Select the listing from **Snowflake Public Data Products**
3. Click **Get** and accept the terms
4. Database name: `SNOWFLAKE_PUBLIC_DATA` (or use the suggested name)

Both datasets appear instantly, no ETL required.

### 6.2 Example 1: Weather Impact on EV Range

Cold weather reduces EV battery range by 20-40%. Let's explore weather data:

```sql
-- Check what cities are available in the weather sample
SELECT DISTINCT CITY_NAME, COUNTRY_CODE 
FROM WEATHER_SOURCE.STANDARD_TILE.POINT_HISTORY_DAY
ORDER BY COUNTRY_CODE, CITY_NAME;

-- Get average temperature trends (sample includes major global cities)
SELECT 
    YEAR(DATE_VALID_STD) AS year,
    CITY_NAME,
    ROUND(AVG(AVG_TEMPERATURE_AIR_2M_F), 1) AS avg_temp_f,
    ROUND((AVG(AVG_TEMPERATURE_AIR_2M_F) - 32) * 5/9, 1) AS avg_temp_c,
    COUNT(*) AS days_recorded
FROM WEATHER_SOURCE.STANDARD_TILE.POINT_HISTORY_DAY
WHERE COUNTRY_CODE = 'US'
GROUP BY 1, 2
ORDER BY year DESC, CITY_NAME;
```

> **Insight for Pon:** Weather data helps predict seasonal demand. EVs sell better in spring/summer when range anxiety is lower.

### 6.3 Example 2: Economic Indicators for Market Analysis

The Snowflake Public Data includes OECD economic indicators, World Bank data, and more:

```sql
-- Explore available economic timeseries
SELECT DISTINCT 
    ts.VARIABLE_NAME,
    ts.UNIT,
    g.GEO_NAME
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.DATACOMMONS_TIMESERIES ts
JOIN SNOWFLAKE_PUBLIC_DATA.CYBERSYN.GEOGRAPHY_INDEX g
    ON ts.GEO_ID = g.GEO_ID
WHERE g.GEO_NAME ILIKE '%netherlands%'
    AND ts.VARIABLE_NAME ILIKE '%income%' OR ts.VARIABLE_NAME ILIKE '%population%'
LIMIT 20;

-- GDP and economic growth (example query structure)
SELECT 
    DATE,
    VARIABLE_NAME,
    VALUE,
    UNIT
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.DATACOMMONS_TIMESERIES
WHERE GEO_ID = 'country/NLD'
    AND VARIABLE_NAME ILIKE '%gdp%'
ORDER BY DATE DESC
LIMIT 10;
```

### 6.4 Example 3: Energy Prices and Charging Costs

Energy costs directly impact EV ownership economics:

```sql
-- Explore energy data from EIA (US Energy Information Administration)
SELECT DISTINCT 
    VARIABLE_NAME,
    UNIT,
    FREQUENCY
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.EIA_TIMESERIES
WHERE VARIABLE_NAME ILIKE '%electricity%price%'
LIMIT 20;

-- European energy context from ECB/Eurostat
SELECT 
    DATE,
    VARIABLE_NAME,
    VALUE,
    UNIT
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.ECB_TIMESERIES
WHERE VARIABLE_NAME ILIKE '%energy%'
ORDER BY DATE DESC
LIMIT 20;
```

> **Insight for Pon:** When electricity prices drop relative to petrol, EV adoption accelerates.

### 6.5 Example 4: Climate Data for Sustainability Reporting

Track greenhouse gas emissions for ESG reporting:

```sql
-- Climate Watch emissions data
SELECT 
    DATE,
    GEO_ID,
    VARIABLE_NAME,
    VALUE,
    UNIT
FROM SNOWFLAKE_PUBLIC_DATA.CYBERSYN.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
    AND VARIABLE_NAME ILIKE '%transport%' OR VARIABLE_NAME ILIKE '%emission%'
ORDER BY DATE DESC
LIMIT 20;
```

### Key Differentiator

| Platform | Third-party Data Access | Notes |
|----------|------------------------|-------|
| **Snowflake** | 2,500+ datasets, instant access | Zero ETL, zero storage duplication |
| Databricks | Unity Catalog Marketplace | Growing catalog, Delta Sharing based |
| Fabric | OneLake shortcuts | Microsoft ecosystem focused |

### Marketplace Value for Pon

| Use Case | Dataset | Business Impact |
|----------|---------|-----------------|
| **Seasonal demand** | Weather data | Plan inventory for spring EV sales push |
| **Regional targeting** | Demographics | Focus marketing on high-income urban areas |
| **Pricing strategy** | Energy prices | Time promotions with low electricity rates |
| **ESG reporting** | Climate data | Quantify CO2 reduction from EV fleet |
| **Economic outlook** | GDP/employment | Forecast demand based on economic health |

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 7: Streamlit Dashboard

**Duration: 15 minutes**

> **Why This Matters for Pon**
> 
> The PDF use case has a clear objective: *"Welke regio heeft de snelste groei van EV's en zie je dat terug in aantal beschikbare laadpalen?"* (Which region has the fastest EV growth and does that correlate with available charging points?)
> 
> This dashboard **directly answers that question** with interactive visualizations showing:
> - Regional EV adoption rankings (which regions lead the transition)
> - Infrastructure gap analysis (where charging supply lags EV demand)
> - Growth trends (how quickly is adoption accelerating)
> 
> Leadership gets a single screen to make strategic decisions about dealership inventory, charging partnerships, and regional marketing focus - no waiting for analysts to pull reports.

Build an interactive dashboard directly in Snowflake. No external hosting, no separate deployment. This is the visualization layer that brings everything together — and **answers the business question**.

> **💡 Talking Point:** The Streamlit app runs natively in Snowflake. No Docker, no Kubernetes, no separate infrastructure. It queries your Dynamic Tables directly and inherits all your governance controls.

### 7.1 Create the Streamlit App

1. In Snowsight, go to **Projects** > **Streamlit**
2. Click **+ Streamlit App**
3. Configure:
   - **Name:** `EV_TRANSITION_DASHBOARD`
   - **Warehouse:** `PON_ANALYTICS_WH`
   - **Database/Schema:** `PON_EV_LAB.ANALYTICS`
4. Click **Create**

### 7.2 Add the Dashboard Code

Copy the code from `streamlit_app.py` in this repository. The full source code creates a professional dashboard with:

- **Pon-branded SVG banner** matching the repo style
- **5 tabs**: Regional Analysis, EV vs Infrastructure, Trends & Insights, Fuel Mix, Platform
- **Dutch postal code mapping** for human-readable region names
- **Data-driven insights** from live Dynamic Table queries
- **Business question answered** in Tab 2 with explicit correlation analysis
- **"Why Snowflake?" comparison table** highlighting platform strengths

The dashboard queries these Dynamic Tables:
- `CURATED.EV_BY_REGION` - Regional EV adoption metrics
- `CURATED.CHARGING_BY_AREA` - Charging infrastructure
- `ANALYTICS.EV_YOY_GROWTH` - Year-over-year trends
- `ANALYTICS.EV_INFRASTRUCTURE_CORRELATION` - Infrastructure gap analysis

> **Note:** The complete Streamlit code is ~450 lines. See `streamlit_app.py` for the full implementation.

### 7.3 Run the App

Click **Run** in the top-right corner. Your dashboard is now live.

### Checkpoint

You should see an interactive dashboard with:
- Pon-branded banner with logo, EV car, and Snowflake icon
- Key metrics: Total EVs, National EV Share, Top Region, Charging Points, Regions
- **Tab 1**: Regional EV adoption bar chart and top 5 regions
- **Tab 2**: EV vs Infrastructure correlation analysis
- **Tab 3**: Trends with year-over-year growth and insights
- **Tab 4**: Fuel mix distribution (Electric, Petrol, Diesel)
- **Tab 5**: Platform architecture and "Why Snowflake?" comparison

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 8: Wrap-up & Discussion

**Duration: 10 minutes**

### What We Built

| Component | Snowflake Feature | Benefit |
|-----------|-------------------|---------|
| API Ingestion | External Access + UDFs | No external tools needed |
| Data Pipeline | Dynamic Tables | Zero orchestration |
| Scaling | Multi-cluster Warehouse | Instant, automatic |
| Cost Control | Resource Monitors | Predictable spend |
| Data Sharing | Secure Shares | Live data, no copies |
| Data Enrichment | Marketplace | Instant third-party data |
| Dashboard | Streamlit in Snowflake | No separate hosting |

### Snowflake Strengths Demonstrated

**What we showcased:**
- Instant warehouse startup (no cold-start delays)
- Zero-copy data sharing (works across clouds)
- Streamlit natively integrated (no separate deployment)
- Dynamic Tables (declarative, SQL-native pipelines)
- External Access (API calls without middleware)

**Why this matters for Pon:**
- Faster time-to-insight (no infrastructure setup)
- Lower operational overhead (no cluster management)
- Simpler data collaboration (share with dealers instantly)
- Unified platform (data engineering to dashboards)

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
