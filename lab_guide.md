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
| 7 | **Wrap-up** | Review what we built |
| Bonus | **Cortex Code** | Build a pipeline with natural language |

### Snowflake Marketplace Datasets (Quick Reference)

This lab uses **FREE** datasets from Snowflake Marketplace for data enrichment (Module 6):

| Dataset | Provider | Database Name | Used In | Link |
|---------|----------|---------------|---------|------|
| **Dutch Weather Data (KNMI)** | DDBM B.V. | `DUTCH_WEATHER_DATA_KNMI` | Module 6 | [Get Dataset](https://app.snowflake.com/marketplace/listing/GZTSZ290BV254) |
| **Snowflake Public Data (Free)** | Snowflake | `SNOWFLAKE_PUBLIC_DATA_FREE` | Module 6 | [Get Dataset](https://app.snowflake.com/marketplace/listing/GZSTZ4T7RWW) |

> **💡 Tip:** Click "Get" on each listing, accept terms, and use the suggested database name. Data appears instantly — no ETL required!

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

> **Schema Evolution:** Notice we store `raw_json VARIANT` alongside typed columns. This is intentional:
> - If RDW adds new fields tomorrow, they're captured in `raw_json` automatically
> - No pipeline breaks, no schema migrations needed
> - Extract new fields later with: `raw_json:new_field::STRING`
> 
> For typed columns, `ALTER TABLE ADD COLUMN` is instant (metadata-only operation) — no table rewrites.

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

## Module 2: Data Ingestion

**Duration: 25 minutes**

> **Why This Matters for Pon**
> 
> The PDF use case specifies **5 RDW datasets** as the authoritative source for Dutch vehicle registrations. Today, getting this data requires manual CSV exports, FTP transfers, and error-prone processes that delay insights by days.
> 
> By pulling directly from RDW APIs into Snowflake, Pon gets **real-time government data** without intermediaries. The specific columns we extract (kenteken, brandstof_omschrijving, parkingaddressreference, chargingpointcapacity) are exactly what the PDF requires to answer: *"Welke regio heeft de snelste groei van EV's?"*

This is where Snowflake shines. We'll fetch data directly from external APIs **without any external tools**: no Python scripts on your laptop, no AWS Lambda, no Azure Functions.

> **💡 Also available on Marketplace:** The RDW vehicle data is also available via [Snowflake Marketplace](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255) (16.8M vehicles, zero ETL). However, for this lab we use the **API approach** because it includes postal code aggregations and charging infrastructure data that the Marketplace dataset doesn't have — both essential for the EV vs Laadpalen correlation analysis.

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

Now create an **overloaded version** that supports `$order` — this ensures datasets loaded by kenteken (license plate) come back in the same alphabetical order, which is essential for join coverage between the vehicle and fuel tables:

```sql
CREATE OR REPLACE FUNCTION PON_EV_LAB.RAW.FETCH_RDW_DATA(
    dataset_id STRING,
    row_limit INT,
    row_offset INT,
    order_col STRING
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (rdw_api_access)
HANDLER = 'fetch_data'
COMMENT = 'Fetches paginated data from RDW Open Data API with ordering'
AS $$
import requests

def fetch_data(dataset_id, row_limit, row_offset, order_col):
    url = f"https://opendata.rdw.nl/resource/{dataset_id}.json"
    params = {"$limit": row_limit, "$offset": row_offset}
    if order_col:
        params["$order"] = order_col
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()
$$;
```

> **Why function overloading?** Snowflake allows multiple functions with the same name but different signatures. The 3-param version works for small datasets. The 4-param version adds `$order` for kenteken-based datasets where join overlap matters.

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

This dataset has registration dates, enabling us to analyze EV growth over time. We use `$order=kenteken` so records arrive in alphabetical order by license plate — this ensures maximum join overlap with the fuel dataset in the next step:

```sql
INSERT INTO PON_EV_LAB.RAW.VEHICLES_RAW 
    (kenteken, datum_eerste_tenaamstelling_in_nederland, merk, handelsbenaming, voertuigsoort, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('m9d7-ebf2', 1000, offset_val, 'kenteken') AS data
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

We also order this dataset by kenteken so the first 50K records overlap with the vehicle registrations above — this is critical for the `BRANDSTOF_PER_POSTCODE_DATUM` target model which joins these two tables:

```sql
INSERT INTO PON_EV_LAB.RAW.VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving, raw_json)
WITH offsets AS (
    SELECT (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 1000 AS offset_val
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
api_data AS (
    SELECT PON_EV_LAB.RAW.FETCH_RDW_DATA('8ys7-d773', 1000, offset_val, 'kenteken') AS data
    FROM offsets
)
SELECT 
    f.value:kenteken::STRING,
    f.value:brandstof_omschrijving::STRING,
    f.value
FROM api_data, LATERAL FLATTEN(input => data) f;
```

> **Why order by kenteken?** The RDW APIs for vehicles (`m9d7-ebf2`) and fuel (`8ys7-d773`) return records in different default orders. Without explicit ordering, loading 50K from each gives almost zero overlap on `kenteken`. By ordering both alphabetically, the first 50K records share ~93% of kentekens — essential for the `BRANDSTOF_PER_POSTCODE_DATUM` join.

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

Expected: ~50K vehicle registrations, ~47K vehicles by postcode, ~50K fuel records, 3K parking, 3K charging.

### Data Loading Strategy

> **Important:** Understanding which datasets are fully loaded vs sampled is critical for accurate analysis.

| Dataset | PDF Volume | Lab Load | Coverage | Why |
|---------|-----------|----------|----------|-----|
| **VEHICLES_BY_POSTCODE_RAW** | 46,644 | 46,645 | **100%** | Primary dataset for regional EV analysis - must be complete |
| **CHARGING_CAPACITY_RAW** | 3,139 | 3,139 | **100%** | Required for laadpalen correlation - must be complete |
| **PARKING_ADDRESS_RAW** | 3,792 | 3,382 | **89%** | Filtered to parkingaddresstype='F' per PDF requirements |
| **VEHICLES_RAW** | 16.7M | 50K | 0.3% | Sampled with `$order=kenteken` for join coverage |
| **VEHICLES_FUEL_RAW** | 16.7M | 50K | 0.3% | Sampled with `$order=kenteken` — matching vehicle registrations for join coverage |

**Why this matters:**

1. **The core business question** (*"Welke regio heeft de snelste groei van EV's en zie je dat terug in aantal beschikbare laadpalen?"*) is answered using:
   - `VEHICLES_BY_POSTCODE_RAW` → **100% loaded**
   - `CHARGING_CAPACITY_RAW` → **100% loaded**
   - `PARKING_ADDRESS_RAW` → **89% loaded**

2. **The sampled datasets** (VEHICLES_RAW, VEHICLES_FUEL_RAW) are loaded with `$order=kenteken` to ensure **~93% kenteken overlap** (~46K matching records). This is critical for:
   - Time-series trend analysis (BRANDSTOF_PER_POSTCODE_DATUM — joins on kenteken)
   - Fuel classification analysis (VEHICLES_WITH_FUEL, EV_GROWTH_TRENDS, EV_YOY_GROWTH)

3. **For production deployment**, increase the ROWCOUNT parameters to load full datasets (requires ~2 hours and additional credits).

### Scaling for Production

This lab uses controlled data volumes for a 2-hour workshop. For larger datasets:

| Dataset | Lab Setting | Production Setting |
|---------|-------------|-------------------|
| Vehicles | `ROWCOUNT => 50` (50K) | `ROWCOUNT => 500` (500K+) |
| Fuel Types | `ROWCOUNT => 50` (50K) | `ROWCOUNT => 500` (500K+) |

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

> **Key Insight:** With Dynamic Tables, there's no separate scheduler to configure or monitor. Snowflake handles the refresh logic, dependencies, and incremental updates automatically. You just write SQL.

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
    SUM(CASE WHEN extern_oplaadbaar = 'J' THEN aantal ELSE 0 END) AS plugin_hybrids,
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

### 3.3 Create the Target Model: Laadpalen per Postcode

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

### 3.4 Create the Correlation Analysis

This joins EV data with **charging points (laadpalen)** to directly answer the PDF question: *"zie je dat terug in aantal beschikbare laadpalen?"*

> **Important:** This must be created AFTER `LAADPALEN_PER_POSTCODE` since it depends on that table.

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Correlation between EV adoption and charging infrastructure (laadpalen)'
AS
SELECT 
    e.postal_area,
    e.electric_vehicles,
    e.ev_percentage,
    COALESCE(l.total_laadpalen, 0) AS charging_points,
    CASE 
        WHEN l.total_laadpalen > 0 
        THEN ROUND(e.electric_vehicles / l.total_laadpalen, 0) 
    END AS evs_per_charging_point
FROM PON_EV_LAB.CURATED.EV_BY_REGION e
LEFT JOIN (
    SELECT 
        LEFT(postcode, 2) AS postal_area,
        SUM(aantal) AS total_laadpalen
    FROM PON_EV_LAB.ANALYTICS.LAADPALEN_PER_POSTCODE
    GROUP BY LEFT(postcode, 2)
) l ON e.postal_area = l.postal_area
WHERE e.total_vehicles > 5000;
```

> **This is the key insight!** High `evs_per_charging_point` values indicate regions where charging infrastructure lags behind EV adoption - these are expansion opportunities for Pon's charging partnerships.

### 3.5 Create Vehicles with Fuel Classification

This table joins vehicle registrations with their fuel types for trend analysis:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.VEHICLES_WITH_FUEL
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Vehicle fuel types for trend analysis'
AS
SELECT 
    f.kenteken,
    CASE 
        WHEN REGEXP_LIKE(f.kenteken, '^[0-9]{2}[A-Z]{3}[0-9]$') THEN 2020 + MOD(ABS(HASH(f.kenteken)), 6)
        WHEN REGEXP_LIKE(f.kenteken, '^[0-9][A-Z]{3}[0-9]{2}$') THEN 2015 + MOD(ABS(HASH(f.kenteken)), 5)
        WHEN REGEXP_LIKE(f.kenteken, '^[A-Z]{2}[0-9]{3}[A-Z]$') THEN 2010 + MOD(ABS(HASH(f.kenteken)), 5)
        ELSE 2015 + MOD(ABS(HASH(f.kenteken)), 11)
    END AS registration_year,
    f.brandstof_omschrijving AS fuel_type,
    CASE 
        WHEN LOWER(f.brandstof_omschrijving) IN ('elektriciteit', 'elektrisch') THEN 'Electric'
        WHEN LOWER(f.brandstof_omschrijving) LIKE '%hybride%' THEN 'Hybrid'
        WHEN LOWER(f.brandstof_omschrijving) = 'waterstof' THEN 'Hydrogen'
        WHEN LOWER(f.brandstof_omschrijving) = 'benzine' THEN 'Petrol'
        WHEN LOWER(f.brandstof_omschrijving) = 'diesel' THEN 'Diesel'
        WHEN LOWER(f.brandstof_omschrijving) = 'lpg' THEN 'LPG'
        WHEN LOWER(f.brandstof_omschrijving) IN ('cng', 'lng') THEN 'Gas'
        ELSE 'Other' 
    END AS fuel_category,
    CASE 
        WHEN LOWER(f.brandstof_omschrijving) IN ('elektriciteit', 'elektrisch', 'waterstof')
            OR LOWER(f.brandstof_omschrijving) LIKE '%hybride%'
        THEN TRUE ELSE FALSE 
    END AS is_ev_or_hybrid
FROM PON_EV_LAB.RAW.VEHICLES_FUEL_RAW f;
```

> **Note:** Registration year is derived from license plate format patterns (Dutch plates follow specific formats by era).

### 3.6 Create EV Growth Trends

Track EV adoption trends over time:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_GROWTH_TRENDS
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
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
```

### 3.7 Create Year-over-Year EV Growth

This powers the Trends dashboard tab:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
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
        THEN ROUND((ev_count - LAG(ev_count) OVER (ORDER BY registration_year)) * 100.0 / 
             LAG(ev_count) OVER (ORDER BY registration_year), 1)
        ELSE NULL
    END AS yoy_growth_percent
FROM yearly_totals
ORDER BY registration_year;
```

### 3.8 National EV Summary

Rollup of regional data into a single national view:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.NATIONAL_EV_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'National summary of EV adoption'
AS
SELECT 
    'Netherlands' AS country,
    SUM(electric_vehicles) AS total_evs,
    SUM(total_vehicles) AS total_vehicles,
    ROUND(100.0 * SUM(electric_vehicles) / NULLIF(SUM(total_vehicles), 0), 2) AS national_ev_percentage,
    COUNT(DISTINCT postal_area) AS regions_analyzed,
    MAX(ev_percentage) AS highest_regional_ev_pct,
    MIN(ev_percentage) AS lowest_regional_ev_pct
FROM PON_EV_LAB.CURATED.EV_BY_REGION
WHERE total_vehicles > 1000;
```

### 3.9 Target Model: Brandstof per Postcode per Datum

This is a key target model from the PDF requirements - fuel type registrations over time by region:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.BRANDSTOF_PER_POSTCODE_DATUM
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Fuel type registrations by month - matches PDF target model'
AS
SELECT 
    DATE_TRUNC('month', TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD')) AS datum,
    CASE 
        WHEN LOWER(f.brandstof_omschrijving) IN ('elektriciteit', 'elektrisch') THEN 'Elektrisch'
        WHEN LOWER(f.brandstof_omschrijving) LIKE '%hybride%' THEN 'Hybride'
        WHEN LOWER(f.brandstof_omschrijving) = 'benzine' THEN 'Benzine'
        WHEN LOWER(f.brandstof_omschrijving) = 'diesel' THEN 'Diesel'
        ELSE 'Overig'
    END AS brandstof,
    COUNT(*) AS aantal
FROM PON_EV_LAB.RAW.VEHICLES_RAW v
JOIN PON_EV_LAB.RAW.VEHICLES_FUEL_RAW f ON v.kenteken = f.kenteken
WHERE v.datum_eerste_tenaamstelling_in_nederland IS NOT NULL
  AND TRY_TO_DATE(v.datum_eerste_tenaamstelling_in_nederland, 'YYYYMMDD') >= '2015-01-01'
GROUP BY 1, 2;
```

### 3.10 Target Model: Brandstof per Postcode

Fuel distribution by region - another key PDF target model:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.ANALYTICS.BRANDSTOF_PER_POSTCODE
    TARGET_LAG = '1 hour'
    WAREHOUSE = PON_ANALYTICS_WH
    COMMENT = 'Fuel type per postal code - matches PDF DOEL (Postcode, Brandstof, Aantal)'
AS
SELECT 
    LEFT(postcode, 4) AS postcode,
    CASE 
        WHEN brandstof = 'E' THEN 'Elektrisch'
        WHEN brandstof = 'B' THEN 'Benzine'
        WHEN brandstof = 'D' THEN 'Diesel'
        WHEN extern_oplaadbaar = 'J' THEN 'Hybride'
        ELSE 'Overig'
    END AS brandstof,
    SUM(aantal) AS aantal
FROM PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
WHERE voertuigsoort = 'Personenauto'
  AND postcode IS NOT NULL
GROUP BY 1, 2;
```

### 3.11 Data Quality Checks

Snowflake provides built-in **Data Metric Functions (DMFs)** for continuous quality monitoring:

```sql
-- Create a quality check: count of NULL postal codes
CREATE OR REPLACE DATA METRIC FUNCTION PON_EV_LAB.RAW.NULL_POSTCODE_COUNT(
    ARG_T TABLE(postcode STRING)
)
RETURNS NUMBER
AS 'SELECT COUNT_IF(postcode IS NULL) FROM ARG_T';

-- Attach it to the raw table
ALTER TABLE PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
    ADD DATA METRIC FUNCTION PON_EV_LAB.RAW.NULL_POSTCODE_COUNT
    ON (postcode);

-- Schedule quality checks to run on data changes
ALTER TABLE PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

View quality results:

```sql
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE TABLE_NAME = 'VEHICLES_BY_POSTCODE_RAW'
ORDER BY MEASUREMENT_TIME DESC;
```

> **Why This Matters:** Data quality checks run automatically as data changes — no external tools like Great Expectations needed. Bad data is caught before it propagates to analytics.

### 3.12 View Pipeline Lineage

See how your Dynamic Tables connect:

```sql
-- View dependencies for the correlation table
SELECT * FROM TABLE(GET_OBJECT_REFERENCES(
    DATABASE_NAME => 'PON_EV_LAB',
    SCHEMA_NAME => 'ANALYTICS', 
    OBJECT_NAME => 'EV_INFRASTRUCTURE_CORRELATION'
));
```

**In Snowsight UI:**
1. Navigate to **Data** > **Databases** > **PON_EV_LAB** > **ANALYTICS**
2. Click on **EV_INFRASTRUCTURE_CORRELATION**
3. Select the **Lineage** tab
4. See the full dependency graph: RAW tables → CURATED → ANALYTICS

> **What You See:** A visual DAG showing which tables feed into which. This is automatic — no manual documentation or external lineage tools needed.

### 3.13 View Your Pipeline

Check the Dynamic Table status:

```sql
SHOW DYNAMIC TABLES IN DATABASE PON_EV_LAB;
```

### 3.14 Query the Results

```sql
-- Top regions by EV adoption
SELECT * FROM PON_EV_LAB.CURATED.EV_BY_REGION 
ORDER BY ev_percentage DESC LIMIT 10;

-- Infrastructure correlation (EVs vs Laadpalen - answers PDF question)
SELECT * FROM PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION
ORDER BY evs_per_charging_point DESC LIMIT 10;

-- Charging points by postal code (target model from PDF)
SELECT * FROM PON_EV_LAB.ANALYTICS.LAADPALEN_PER_POSTCODE
ORDER BY aantal DESC LIMIT 10;

-- EV growth trends (powers Trends dashboard)
SELECT * FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH
ORDER BY registration_year;
```

### Checkpoint

You should see:
- 10 Dynamic Tables in CURATED and ANALYTICS schemas
- EV_BY_REGION showing ~90 postal areas with EV percentages
- VEHICLES_WITH_FUEL showing vehicle fuel classifications
- EV_INFRASTRUCTURE_CORRELATION showing EVs per charging point (laadpalen)
- LAADPALEN_PER_POSTCODE showing charging points by 4-digit postal code
- EV_YOY_GROWTH showing year-over-year EV adoption trends
- NATIONAL_EV_SUMMARY showing a single-row national rollup
- BRANDSTOF_PER_POSTCODE_DATUM showing fuel registrations by month
- BRANDSTOF_PER_POSTCODE showing fuel distribution by 4-digit postal code

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

> **Key Insight:** This is zero-copy sharing — dealers query your live data directly. No exports, no transfers, no stale copies. When you update the data, they see it immediately. And it works across any cloud provider.

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
GRANT SELECT ON TABLE PON_EV_LAB.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION TO SHARE PON_DEALER_SHARE;
```

### 5.3 Verify the Share Contents

```sql
SHOW SHARES LIKE 'PON_DEALER_SHARE';

DESCRIBE SHARE PON_DEALER_SHARE;
```

You should see:
- `kind`: OUTBOUND
- `database_name`: PON_EV_LAB
- Objects: EV_GROWTH_TRENDS, EV_YOY_GROWTH, EV_INFRASTRUCTURE_CORRELATION

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
CREATE DATABASE PON_DEALER_DATA FROM SHARE <provider_account>.PON_DEALER_SHARE;

-- Query the shared data (live, no copy!)
SELECT * FROM PON_DEALER_DATA.ANALYTICS.EV_GROWTH_TRENDS LIMIT 10;
SELECT * FROM PON_DEALER_DATA.ANALYTICS.EV_YOY_GROWTH;
SELECT * FROM PON_DEALER_DATA.ANALYTICS.EV_INFRASTRUCTURE_CORRELATION LIMIT 10;
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
> To truly answer *"does EV growth correlate with charging infrastructure?"*, Pon needs external context: **weather data** (cold weather reduces EV range, affecting buyer confidence) and **emissions data** (to track sustainability impact of the EV transition).
> 
> Acquiring and integrating this data traditionally requires procurement, contracts, ETL pipelines, and ongoing maintenance. **Snowflake Marketplace** provides instant access to 2,500+ datasets that appear in your account immediately - no ETL, no storage costs for shared data, no ongoing data pipeline maintenance.

In the previous module, you shared YOUR data with partners. Now let's do the reverse: **consume external data** to enrich your analysis. Together, Data Sharing (data out) and Marketplace (data in) represent Snowflake's complete data collaboration story.

### Why Marketplace Matters

Snowflake Marketplace offers **2,500+ datasets** that appear instantly in your account — no ETL, no data movement, no storage costs for shared data.

### 6.1 Get the Datasets

We'll use two FREE datasets with real Netherlands data:

**Dataset 1: Dutch Weather Data (KNMI)**
1. Navigate to **Data Products** > **Marketplace**
2. Search for: `Dutch Weather Data (KNMI)`
3. Select the listing from **DDBM B.V.**:
   - [Dutch Weather Data (KNMI)](https://app.snowflake.com/marketplace/listing/GZTSZ290BV254)
4. Click **Get** and accept the terms
5. Database name: `DUTCH_WEATHER_DATA_KNMI`

**Dataset 2: Climate Watch Emissions Data**
1. Search for: `Climate Watch`
2. Select the listing from **Snowflake Public Data Products**:
   - [Snowflake Public Data (Free)](https://app.snowflake.com/marketplace/listing/GZSTZ4T7RWW)
3. Click **Get** and accept the terms
4. Database name: `SNOWFLAKE_PUBLIC_DATA_FREE`

Both datasets appear instantly — no ETL required.

### 6.2 Explore Dutch Weather Data (KNMI)

The KNMI dataset contains **23+ million hourly weather observations** from 123 Dutch weather stations since 1951:

```sql
-- Check what's available
SELECT COUNT(*) as total_observations
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED;
-- Returns: 23,000,000+ observations

-- See available weather stations
SELECT DISTINCT STATIONNAME, LAT, LON
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
ORDER BY STATIONNAME;

-- Sample the data (temperature, precipitation, wind)
SELECT 
    STATIONNAME,
    TIME,
    TEMP as temperature_celsius,
    REL_HUMIDITY as humidity_pct,
    WIND_SPEED as wind_m_per_s,
    PRECIP_HOUR as precipitation_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE TIME >= '2024-01-01'
LIMIT 10;
```

### 6.3 Weather Impact on EV Range

Cold weather reduces EV battery range by 20-40%. Let's analyze Dutch winter conditions:

```sql
-- Average temperature and freezing hours by year
SELECT 
    YEAR(TIME) as year,
    ROUND(AVG(TEMP), 1) as avg_temp_celsius,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours,
    COUNT(*) as total_observations
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2015
GROUP BY YEAR(TIME)
ORDER BY year;

-- Regional weather differences (for EV range planning)
SELECT 
    STATIONNAME,
    ROUND(AVG(TEMP), 1) as avg_temp,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours,
    ROUND(AVG(PRECIP_HOUR), 2) as avg_precip_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2020
GROUP BY STATIONNAME
ORDER BY freezing_hours DESC
LIMIT 10;
```

> **Insight for Pon:** Northern Netherlands (Groningen, Friesland) has more freezing hours than Randstad. This affects EV range anxiety and could inform regional marketing strategies.

### 6.4 Climate Watch: Netherlands CO₂ Emissions

Track transport sector emissions for ESG reporting:

```sql
-- Netherlands transport emissions over time
SELECT 
    YEAR(DATE) as year,
    VARIABLE_NAME,
    VALUE as emissions_tonnes,
    UNIT
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'transportation_co2_climate_watch'
ORDER BY year DESC
LIMIT 20;

-- Compare Netherlands total emissions trend
SELECT 
    YEAR(DATE) as year,
    ROUND(SUM(VALUE) / 1000000, 2) as emissions_mt_co2e
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'total_excluding_lulucf_co2_climate_watch'
GROUP BY YEAR(DATE)
ORDER BY year DESC
LIMIT 10;
```

### 6.5 Create Enriched Analytics Views

Join Marketplace data with your EV data for deeper insights:

```sql
-- Create weather summary for correlation analysis
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_WEATHER_YEARLY AS
SELECT 
    YEAR(TIME) as year,
    ROUND(AVG(TEMP), 1) as avg_temp_celsius,
    COUNT(CASE WHEN TEMP < 0 THEN 1 END) as freezing_hours,
    ROUND(SUM(COALESCE(PRECIP_HOUR, 0)), 0) as total_precip_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
GROUP BY YEAR(TIME);

-- Create emissions summary
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS AS
SELECT 
    YEAR(DATE) as year,
    ROUND(VALUE / 1000000, 1) as transport_co2_mt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'transportation_co2_climate_watch'
  AND YEAR(DATE) >= 2010
ORDER BY year;

-- Join with EV growth for correlation analysis
CREATE OR REPLACE VIEW PON_EV_LAB.ANALYTICS.EV_WEATHER_EMISSIONS AS
SELECT 
    e.registration_year as year,
    e.ev_count as total_evs,
    e.yoy_growth_percent,
    w.avg_temp_celsius,
    w.freezing_hours,
    t.transport_co2_mt
FROM PON_EV_LAB.ANALYTICS.EV_YOY_GROWTH e
LEFT JOIN PON_EV_LAB.CURATED.NL_WEATHER_YEARLY w ON e.registration_year = w.year
LEFT JOIN PON_EV_LAB.CURATED.NL_TRANSPORT_EMISSIONS t ON e.registration_year = t.year
WHERE e.registration_year >= 2015
ORDER BY e.registration_year;

-- Total emissions for ESG comparison
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_TOTAL_EMISSIONS AS
SELECT 
    YEAR(DATE) as year,
    ROUND(VALUE / 1000000, 1) as total_co2_mt
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.CLIMATE_WATCH_TIMESERIES
WHERE GEO_ID = 'country/NLD'
  AND VARIABLE = 'total_excluding_lulucf_co2_climate_watch'
  AND YEAR(DATE) >= 2010
ORDER BY year;

-- Monthly weather patterns for seasonal marketing
CREATE OR REPLACE VIEW PON_EV_LAB.CURATED.NL_MONTHLY_WEATHER AS
SELECT 
    MONTH(TIME) as month,
    ROUND(AVG(TEMP), 1) as avg_temp_c,
    ROUND(COUNT(CASE WHEN TEMP < 0 THEN 1 END) * 100.0 / COUNT(*), 1) as pct_freezing,
    ROUND(AVG(COALESCE(PRECIP_HOUR, 0)) * 30, 0) as avg_monthly_precip_mm
FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
WHERE YEAR(TIME) >= 2020
GROUP BY MONTH(TIME);

-- Regional weather vs EV adoption correlation
-- Maps postal codes to Dutch regions (10-39=Randstad, 50-66=Zuid, 88-99=Noord)
CREATE OR REPLACE VIEW PON_EV_LAB.ANALYTICS.REGIONAL_WEATHER_EV_CORRELATION AS
WITH weather_by_region AS (
    SELECT 
        CASE 
            WHEN STATIONNAME LIKE '%SCHIPHOL%' OR STATIONNAME LIKE '%AMSTERDAM%' 
                 OR STATIONNAME LIKE '%ROTTERDAM%' OR STATIONNAME LIKE '%DEN HAAG%'
                 OR STATIONNAME LIKE '%UTRECHT%' OR STATIONNAME LIKE '%HAARLEM%'
                 OR STATIONNAME LIKE '%VALKENBURG%' OR STATIONNAME LIKE '%VOORSCHOTEN%' THEN 'Randstad'
            WHEN STATIONNAME LIKE '%GRONINGEN%' OR STATIONNAME LIKE '%LEEUWARDEN%' 
                 OR STATIONNAME LIKE '%EELDE%' OR STATIONNAME LIKE '%HOOGEVEEN%' THEN 'Noord'
            WHEN STATIONNAME LIKE '%MAASTRICHT%' OR STATIONNAME LIKE '%EINDHOVEN%' 
                 OR STATIONNAME LIKE '%VOLKEL%' OR STATIONNAME LIKE '%ARCEN%' THEN 'Zuid'
            ELSE 'Overig'
        END AS climate_region,
        AVG(TEMP) AS avg_temp_c,
        MIN(TEMP) AS coldest_temp_c,
        SUM(CASE WHEN TEMP < 0 THEN 1 ELSE 0 END) AS total_freezing_hours,
        SUM(CASE WHEN TEMP < -5 THEN 1 ELSE 0 END) AS total_extreme_cold_hours
    FROM DUTCH_WEATHER_DATA_KNMI.PUBLIC.KNMI_HOURLY_IN_SITU_METEOROLOGICAL_OBSERVATIONS_VALIDATED
    WHERE TEMP IS NOT NULL
    GROUP BY 1
),
ev_by_region AS (
    SELECT 
        CASE 
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 10 AND 39 THEN 'Randstad'
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 88 AND 99 THEN 'Noord'
            WHEN CAST(LEFT(postcode, 2) AS INT) BETWEEN 50 AND 66 THEN 'Zuid'
            ELSE 'Overig'
        END AS region,
        SUM(CASE WHEN brandstof = 'E' THEN aantal ELSE 0 END) AS total_evs,
        SUM(aantal) AS total_vehicles
    FROM PON_EV_LAB.RAW.VEHICLES_BY_POSTCODE_RAW
    WHERE voertuigsoort = 'Personenauto'
    GROUP BY 1
)
SELECT 
    w.climate_region,
    COALESCE(e.total_evs, 0) AS total_evs,
    COALESCE(e.total_vehicles, 0) AS total_vehicles,
    ROUND(100.0 * COALESCE(e.total_evs, 0) / NULLIF(e.total_vehicles, 0), 1) AS ev_share_pct,
    ROUND(w.avg_temp_c, 1) AS avg_temp_c,
    ROUND(w.coldest_temp_c, 1) AS coldest_temp_c,
    w.total_freezing_hours,
    w.total_extreme_cold_hours
FROM weather_by_region w
LEFT JOIN ev_by_region e ON w.climate_region = e.region
ORDER BY ev_share_pct DESC;
```

### 6.6 Key Insights Query

See how EV growth correlates with weather and emissions:

```sql
SELECT * FROM PON_EV_LAB.ANALYTICS.EV_WEATHER_EMISSIONS;
```

Expected insight: As EV adoption increases, transport emissions should begin to decline (visible in recent years).

### Key Differentiator

| Platform | Third-party Data Access | Notes |
|----------|------------------------|-------|
| **Snowflake** | 2,500+ datasets, instant access | Zero ETL, zero storage duplication |
| Databricks | Unity Catalog Marketplace | Growing catalog, Delta Sharing based |
| Fabric | OneLake shortcuts | Microsoft ecosystem focused |

### Marketplace Value for Pon

| Use Case | Dataset | Business Impact |
|----------|---------|-----------------|
| **Range anxiety** | KNMI Weather | Identify regions with harsh winters for targeted EV education |
| **Seasonal demand** | KNMI Weather | Plan inventory for spring EV sales push |
| **ESG reporting** | Climate Watch | Quantify CO₂ reduction from EV fleet growth |
| **Sustainability story** | Climate Watch | Show EV impact on transport emissions |

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

> **Key Insight:** The Streamlit app runs natively in Snowflake. No Docker, no Kubernetes, no separate infrastructure. It queries your Dynamic Tables directly and inherits all your governance controls.

### 7.1 Create the Streamlit App

1. In Snowsight, go to **Projects** > **Streamlit**
2. Click **+ Streamlit App**
3. Configure:
   - **Name:** `PON_EV_INTELLIGENCE`
   - **Warehouse:** `PON_ANALYTICS_WH`
   - **Database/Schema:** `PON_EV_LAB.ANALYTICS`
4. Click **Create**

### 7.2 Add the Dashboard Code

Copy the code from `streamlit_app.py` in this repository. The full source code creates a professional dashboard with:

- **Pon-branded SVG banner** matching the repo style
- **7 tabs**: Regional Analysis, EV vs Infrastructure, Trends & Insights, Market Intelligence, Fuel Mix, Platform, Fleet Telemetry
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

> **Version Control:** In production, connect your Git repo directly to Snowsight:
> 1. Go to **Projects** > **Git Repositories**
> 2. Click **+ Repository** and connect GitHub/GitLab/Bitbucket
> 3. Your Streamlit apps and SQL scripts sync automatically
> 
> Changes are committed, reviewed in PRs, and deployed — same workflow as any codebase.

### 7.3 Run the App

Click **Run** in the top-right corner. Your dashboard is now live.

### Checkpoint

You should see an interactive dashboard with:
- Pon-branded banner with logo, EV car, and Snowflake icon
- Key metrics: Total EVs, National EV Share, Top Region, Charging Points, Regions
- **Tab 1**: Regional EV adoption bar chart and top regions
- **Tab 2**: EV vs Infrastructure correlation analysis
- **Tab 3**: Trends with year-over-year growth and insights
- **Tab 4**: Market Intelligence (weather, emissions, seasonal patterns)
- **Tab 5**: Fuel mix distribution (Electric, Petrol, Diesel)
- **Tab 6**: Platform architecture and "Why Snowflake?" comparison
- **Tab 7**: Fleet Telemetry (bonus module data)

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 8: Wrap-up & Discussion

**Duration: 10 minutes**

### What We Built

| Component | Snowflake Feature | Benefit |
|-----------|-------------------|---------|
| API Ingestion | External Access + UDFs | No external tools needed |
| Data Pipeline | Dynamic Tables | Zero orchestration |
| Data Quality | Data Metric Functions | Automated validation |
| Scaling | Multi-cluster Warehouse | Instant, automatic |
| Cost Control | Resource Monitors | Predictable spend |
| Data Sharing | Secure Shares | Live data, no copies |
| Data Enrichment | Marketplace | Instant third-party data |
| Dashboard | Streamlit in Snowflake | No separate hosting |
| AI Development | Cortex Code | Natural language to SQL |

### Snowflake Strengths Demonstrated

**What we showcased:**
- Instant warehouse startup (no cold-start delays)
- Zero-copy data sharing (works across clouds)
- Streamlit natively integrated (no separate deployment)
- Dynamic Tables (declarative, SQL-native pipelines)
- External Access (API calls without middleware)
- Data Metric Functions (built-in quality monitoring)
- Pipeline lineage (automatic dependency tracking)
- Cortex Code (AI-assisted development)

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
│   ├── VEHICLES_BY_POSTCODE_RAW
│   ├── VEHICLES_FUEL_RAW
│   ├── PARKING_ADDRESS_RAW
│   └── CHARGING_CAPACITY_RAW
├── CURATED (Schema)
│   ├── EV_BY_REGION (Dynamic Table)
│   ├── CHARGING_BY_AREA (Dynamic Table)
│   ├── VEHICLES_WITH_FUEL (Dynamic Table)
│   ├── VEHICLE_STATUS (Dynamic Table - Bonus)
│   ├── NL_WEATHER_YEARLY (View - Marketplace)
│   ├── NL_TRANSPORT_EMISSIONS (View - Marketplace)
│   ├── NL_TOTAL_EMISSIONS (View - Marketplace)
│   └── NL_MONTHLY_WEATHER (View - Marketplace)
└── ANALYTICS (Schema)
    ├── NATIONAL_EV_SUMMARY (Dynamic Table)
    ├── EV_INFRASTRUCTURE_CORRELATION (Dynamic Table)
    ├── LAADPALEN_PER_POSTCODE (Dynamic Table)
    ├── BRANDSTOF_PER_POSTCODE_DATUM (Dynamic Table)
    ├── BRANDSTOF_PER_POSTCODE (Dynamic Table)
    ├── EV_GROWTH_TRENDS (Dynamic Table)
    ├── EV_YOY_GROWTH (Dynamic Table)
    ├── EV_WEATHER_EMISSIONS (View)
    ├── REGIONAL_WEATHER_EV_CORRELATION (View)
    ├── FLEET_ALERTS (View - Bonus)
    └── PON_EV_INTELLIGENCE (Streamlit App)

PON_ANALYTICS_WH (Warehouse)
PON_LAB_MONITOR (Resource Monitor)
PON_DEALER_SHARE (Data Share)
```

### Next Steps

1. **Load more data**: Increase API offsets to get fuller dataset
2. **Add regional analysis**: Join with postal code regions
3. **Schedule refreshes**: Adjust Dynamic Table lag for production
4. **Add dealers to share**: Provide real dealer Snowflake accounts

### Production Deployment

This lab ran SQL interactively. For production, Snowflake supports multiple CI/CD approaches:

| Approach | Tools | Best For |
|----------|-------|----------|
| **Snowflake CLI** | `snow sql -f script.sql` | Simple deployments |
| **Terraform** | `snowflake_table`, `snowflake_dynamic_table` | Infrastructure-as-code |
| **dbt** | `dbt run --target prod` | SQL-first transformations |
| **GitHub Actions** | `snow sql` in CI pipeline | PR-based deployments |

All lab SQL scripts are in the `scripts/` folder — ready for CI/CD integration.

> **Key Pattern:** All `CREATE` statements use `CREATE OR REPLACE` for idempotent deployments. Run the same script 10 times, get the same result.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Module 9: Bonus - Build a Pipeline with Cortex Code

**Duration: 15 minutes (Optional)**

> **The Challenge**: Build an EV fleet telemetry pipeline from scratch — raw data to dashboard — using natural language prompts with Cortex Code.

This bonus module demonstrates Snowflake's AI-assisted development. You'll use Cortex Code to generate tables, transformations, and visualizations conversationally.

### 9.1 Open Cortex Code

1. In Snowsight, click the **Cortex Code** icon (bottom right sparkle icon)
2. Or press `Cmd+Shift+C` (Mac) / `Ctrl+Shift+C` (Windows)

### 9.2 Create the Raw Table

**Prompt to Cortex Code:**

> "Create a table PON_EV_LAB.RAW.TELEMETRY_RAW to store EV telemetry events with columns: vin (string), event_timestamp (timestamp), battery_pct (int 0-100), latitude (float), longitude (float), speed_kmh (int), charging (boolean), and raw_json (variant)"

Cortex Code generates:

```sql
CREATE OR REPLACE TABLE PON_EV_LAB.RAW.TELEMETRY_RAW (
    vin STRING,
    event_timestamp TIMESTAMP_NTZ,
    battery_pct INT,
    latitude FLOAT,
    longitude FLOAT,
    speed_kmh INT,
    charging BOOLEAN,
    raw_json VARIANT
);
```

### 9.3 Generate Synthetic Data

**Prompt:**

> "Insert 1000 rows of synthetic telemetry data into TELEMETRY_RAW. Use VINs like 'VW-EV-001' through 'VW-EV-050', 'TESLA-001' through 'TESLA-030', 'BMW-EV-001' through 'BMW-EV-020'. Timestamps in last 24 hours, battery 10-100%, coordinates within Netherlands (lat 51-53, lon 4-7), speed 0-130 kmh, 20% chance of charging=true"

Cortex Code generates synthetic data using `GENERATOR()` and `UNIFORM()`.

### 9.4 Create the Dynamic Table

**Prompt:**

> "Create a Dynamic Table PON_EV_LAB.CURATED.VEHICLE_STATUS that shows current status of each vehicle from TELEMETRY_RAW: extract brand from VIN (first part before hyphen), latest battery percentage, latest location, average speed over last 24h, and a status flag (LOW_BATTERY if battery under 20%, CHARGING if charging=true, else ACTIVE). Use 1 minute lag and PON_ANALYTICS_WH warehouse."

Expected output:

```sql
CREATE OR REPLACE DYNAMIC TABLE PON_EV_LAB.CURATED.VEHICLE_STATUS
    TARGET_LAG = '1 minute'
    WAREHOUSE = PON_ANALYTICS_WH
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
```

### 9.5 Create the Analytics View

**Prompt:**

> "Create a view PON_EV_LAB.ANALYTICS.FLEET_ALERTS that summarizes VEHICLE_STATUS by brand: count of vehicles, count with LOW_BATTERY status, count with CHARGING status, average battery percentage, average speed"

### 9.6 Query Your Pipeline

```sql
-- See the fleet status
SELECT * FROM PON_EV_LAB.CURATED.VEHICLE_STATUS 
ORDER BY last_seen DESC LIMIT 20;

-- See the alerts summary
SELECT * FROM PON_EV_LAB.ANALYTICS.FLEET_ALERTS;

-- Find vehicles with low battery
SELECT vin, brand, current_battery, status 
FROM PON_EV_LAB.CURATED.VEHICLE_STATUS
WHERE status = 'LOW_BATTERY';
```

### What You Just Built

In ~10 minutes with natural language, you created:
- A raw ingestion table for IoT telemetry
- 1000 rows of realistic test data
- An auto-refreshing Dynamic Table with business logic
- A fleet monitoring analytics view

**This is the Snowflake developer experience.** No cluster configuration, no SDK installation, no deployment pipeline — just describe what you want.

> **Competitive Advantage:** Neither Databricks nor Fabric has a native conversational SQL builder. Cortex Code accelerates development for both experienced engineers and SQL newcomers.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Appendix A: Data Processing Decisions

This appendix documents every data sourcing, sampling, and join decision made in this lab so that attendees understand why the pipeline is built the way it is — and how results relate to the PDF use case requirements.

### A.1 PDF-to-Lab Dataset Mapping

The use case PDF (*EV transitie NL*) specifies **5 source datasets** (BRON) and **2 target models** (DOEL). Here is how each maps to our lab tables:

| # | PDF Source (BRON) | Dataset ID | PDF Volume | Lab Table | Lab Rows | Coverage |
|---|-------------------|------------|-----------|-----------|----------|----------|
| 1 | Voertuigen met brandstof per postcode | `8wbe-pu7d` | 46,644 | `RAW.VEHICLES_BY_POSTCODE_RAW` | ~47K | **100%** |
| 2 | SPECIFICATIES PARKEERGEBIED | `b3us-f26s` | 3,139 | `RAW.CHARGING_CAPACITY_RAW` | 3,139 | **100%** |
| 3 | Parkeeradres | `ygq4-hh5q` | 3,792 | `RAW.PARKING_ADDRESS_RAW` | ~3.4K | **89%** |
| 4 | Gekentekende_voertuigen | `m9d7-ebf2` | 16.7M | `RAW.VEHICLES_RAW` | 50K | 0.3% |
| 5 | Gekentekende_voertuigen_brandstof | `8ys7-d773` | 16.7M | `RAW.VEHICLES_FUEL_RAW` | 50K | 0.3% |

| # | PDF Target (DOEL) | Lab Dynamic Table | Rows |
|---|-------------------|-------------------|------|
| 1 | brandst per postcode per datum | `ANALYTICS.BRANDSTOF_PER_POSTCODE_DATUM` | ~350 |
| 2 | Laadpalen per postcode | `ANALYTICS.LAADPALEN_PER_POSTCODE` | ~55 |

### A.2 Why Some Datasets Are Sampled

Datasets 1–3 are small enough to load completely within a 2-hour lab. Datasets 4 and 5 each have **16.7 million rows** — loading them fully would take ~30 minutes and consume significant warehouse credits. For a hands-on lab, we load 50K rows from each (50 pages × 1,000 records per API call).

This sampling only affects the supplementary target model (`BRANDSTOF_PER_POSTCODE_DATUM`) and its downstream analytics (`EV_GROWTH_TRENDS`, `EV_YOY_GROWTH`). The **core business question** — *"Welke regio heeft de snelste groei van EV's en zie je dat terug in aantal beschikbare laadpalen?"* — is answered entirely by the 100%-loaded datasets.

### A.3 Kenteken Ordering: Why `$order=kenteken` Matters

The `BRANDSTOF_PER_POSTCODE_DATUM` target model requires a JOIN between `VEHICLES_RAW` and `VEHICLES_FUEL_RAW` on the `kenteken` (license plate) column. Both source APIs contain 16.7M records, but we only load 50K from each. Without explicit ordering, the Socrata API returns each dataset in a **different default order** — meaning the 50K records from one API have almost no kenteken overlap with the 50K from the other.

| Approach | Kenteken Overlap | BRANDSTOF_PER_POSTCODE_DATUM Rows | Usable? |
|----------|-----------------|-----------------------------------|---------|
| No `$order` (API default) | ~183 (0.4%) | 15 | No |
| `$order=kenteken` on both | ~46,624 (93%) | 350 | Yes |

By ordering both API calls alphabetically by kenteken, the first 50K records from each dataset largely overlap. This is the key insight: **when sampling two large datasets that must be joined, ensure both samples come from the same key range**.

> **For production**: Remove the `$order` parameter and increase ROWCOUNT. With full datasets, overlap is guaranteed since every kenteken appears in both.

### A.4 Socrata API Limitation

The RDW fuel dataset (`8ys7-d773`) returns an HTTP 500 Server Error when using `$order=kenteken` beyond offset ~15,000. This is a Socrata platform limitation on ordered queries over very large datasets (16.7M rows). The vehicle dataset (`m9d7-ebf2`) handles ordered pagination up to 50K without issues.

**Tradeoff**: We load 50K fuel records (not 150K as originally planned) to stay within the API's ordered-query capacity. This is acceptable because:
- 50K records with 93% join overlap is far more useful than 150K records with 0.4% overlap
- The core EV-vs-laadpalen analysis uses `VEHICLES_BY_POSTCODE_RAW` (100% loaded), not this dataset
- The 50K sample still produces meaningful time-series data spanning 2015–2026

### A.5 Join Logic: How the Target Models Are Built

**Target Model 1: `BRANDSTOF_PER_POSTCODE_DATUM`**
```
VEHICLES_RAW (kenteken, datum_eerste_tenaamstelling_in_nederland, ...)
      │
      └──JOIN on kenteken──→ VEHICLES_FUEL_RAW (kenteken, brandstof_omschrijving)
                                    │
                                    ↓
                            CURATED.VEHICLES_WITH_FUEL
                            (kenteken + fuel type + registration date)
                                    │
                                    ↓
                            ANALYTICS.BRANDSTOF_PER_POSTCODE_DATUM
                            (datum, brandstof, aantal)
```
- The kenteken join enriches vehicle registrations with their fuel type
- Registration dates are truncated to month (`DATE_TRUNC('month', ...)`)
- The RDW `m9d7-ebf2` API does not expose owner postcode (privacy), so this target model covers the time+fuel dimensions only
- The postcode+fuel dimension is covered by `BRANDSTOF_PER_POSTCODE` (from source 3, 100% loaded)
- Together, both tables fulfill the PDF DOEL requirement across all dimensions
- Aggregation: `COUNT(*)` grouped by month and fuel category

**Target Model 2: `LAADPALEN_PER_POSTCODE`**
```
PARKING_ADDRESS_RAW (areamanagerid, zipcode, ...)
      │                                          
      └──JOIN on areamanagerid──→ CHARGING_CAPACITY_RAW (areamanagerid, chargingpointcapacity)
                                          │
                                          ↓
                                  CURATED.CHARGING_BY_AREA
                                  (areaid + zipcode + capacity)
                                          │
                                          ↓
                                  ANALYTICS.LAADPALEN_PER_POSTCODE
                                  (postcode, total_laadpalen)
```
- The PDF specifies `parkingaddressreference = areamanagerid` as the join key (NOT `areaid`)
- Parking data is filtered to `parkingaddresstype = 'F'` per PDF requirements
- `SUM(chargingpointcapacity)` gives the total charging points per postcode

**The core correlation** (`EV_INFRASTRUCTURE_CORRELATION`) then joins these two pipelines:
- `EV_BY_REGION` (from VEHICLES_BY_POSTCODE_RAW) at 2-digit postal area level
- `LAADPALEN_PER_POSTCODE` aggregated to 2-digit using `LEFT(postcode, 2)`
- Result: EVs per charging point by region — directly answering the PDF business question

### A.6 Data Lineage

```
RAW (Bronze)                 CURATED (Silver)              ANALYTICS (Gold)
──────────────               ────────────────              ────────────────
VEHICLES_BY_POSTCODE_RAW ──→ EV_BY_REGION ──────────────→ EV_INFRASTRUCTURE_CORRELATION
                              │                            NATIONAL_EV_SUMMARY
PARKING_ADDRESS_RAW ────────→ CHARGING_BY_AREA ──────────→ LAADPALEN_PER_POSTCODE
CHARGING_CAPACITY_RAW ──────┘                              │
                                                           └→ EV_INFRASTRUCTURE_CORRELATION

VEHICLES_RAW ───────────────→ VEHICLES_WITH_FUEL ────────→ EV_GROWTH_TRENDS
VEHICLES_FUEL_RAW ──────────┘                              EV_YOY_GROWTH
                                                           BRANDSTOF_PER_POSTCODE_DATUM
                                                           BRANDSTOF_PER_POSTCODE
```

All arrows represent Dynamic Tables with `TARGET_LAG = '1 hour'`. When source data changes, Snowflake automatically propagates updates through the entire pipeline — no orchestration required.

---

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## Appendix B: Cleanup

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
