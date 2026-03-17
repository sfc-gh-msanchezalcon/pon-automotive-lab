<p align="center">
  <img src="assets/banner.svg" alt="Pon Automotive EV Transition Lab" width="100%">
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

## The Business Challenge

**Pon Automotive** manages one of the largest vehicle portfolios in the Netherlands. With the accelerating shift to electric vehicles, leadership needs to answer:

> *"Which region in the Netherlands has the fastest EV growth, and does that correlate with charging infrastructure availability?"*

This insight drives decisions about dealership inventory, charging station partnerships, regional marketing, and fleet transition planning.

### The Answer (Spoiler)

By the end of this lab, your dashboard will show:

| Insight | Finding |
|---------|---------|
| **Highest EV adoption** | Amsterdam area leads with ~25% EV share |
| **Biggest infrastructure gap** | Regions with 500+ EVs per charging location |
| **Correlation** | High EV adoption ≠ sufficient charging — expansion opportunities identified |

This is real RDW data, analyzed in real-time via Dynamic Tables, visualized in Streamlit.

## Module-to-Business Mapping

Each lab module directly addresses a specific Pon business challenge:

| Module | Pon's Pain Point | Snowflake Solution | Business Outcome |
|--------|-----------------|-------------------|------------------|
| **1. Schema Design** | Data silos prevent unified view | Medallion architecture | Single source of truth for leadership |
| **2. API Ingestion** | Manual CSV exports are slow | External Access Integration | Real-time RDW data without intermediaries |
| **3. Dynamic Tables** | Overnight batch jobs create stale data | Automatic refresh with TARGET_LAG | Always-current target models (brandst per postcode, laadpalen per postcode) |
| **4. Cost Control** | 6-hour queries during peak usage | Multi-cluster + Resource Monitors | No one waits, predictable costs |
| **5. Data Sharing** | Manual exports to dealers are ungoverned | Secure Data Sharing | Live, governed access for dealer network |
| **6. Marketplace** | Need external context (weather, demographics) | 2,500+ datasets, zero ETL | Enrich internal data instantly |
| **7. Dashboard** | No unified view for leadership | Streamlit in Snowflake | Answer the core business question directly |

## What You Will Build

A complete data engineering solution using **real Dutch government data**, all within Snowflake. No external tools, no complex orchestration, no cluster management.

By the end you will have:

1. **Live API Data Ingestion**: Real-time data from RDW (Dutch Vehicle Authority) using External Access Integration
2. **Automated Data Pipelines**: Dynamic Tables that refresh automatically with zero orchestration
3. **Cost-Controlled Analytics**: Multi-cluster warehouses with resource monitors
4. **Cross-Organization Data Sharing**: Secure sharing with dealers (no data copies)
5. **Pon EV Intelligence Dashboard**: Streamlit in Snowflake for regional analytics

### Architecture

```
                              SNOWFLAKE ACCOUNT
    ┌────────────────────────────────────────────────────────────────────┐
    │                                                                    │
    │   EXTERNAL APIs                         MARKETPLACE               │
    │   ┌─────────┐                          ┌─────────────┐            │
    │   │ RDW     │                          │ Weather     │            │
    │   │ Open    │                          │ Source      │            │
    │   │ Data    │                          ├─────────────┤            │
    │   └────┬────┘                          │ Snowflake   │            │
    │        │                               │ Public Data │            │
    │        │ External Access               └──────┬──────┘            │
    │        │ Integration                          │ Zero-copy         │
    │        ▼                                      ▼                   │
    │   ┌────────────────────────────────────────────────────────────┐  │
    │   │                        PON_EV_LAB                          │  │
    │   │                                                            │  │
    │   │   RAW               CURATED               ANALYTICS        │  │
    │   │   ┌──────────┐      ┌──────────────┐     ┌─────────────┐  │  │
    │   │   │VEHICLES  │      │VEHICLES_     │     │EV_GROWTH_   │  │  │
    │   │   │_RAW      │─────▶│WITH_FUEL     │────▶│TRENDS       │  │  │
    │   │   ├──────────┤      ├──────────────┤     ├─────────────┤  │  │
    │   │   │FUEL_RAW  │      │CHARGING_     │     │EV_YOY_GROWTH│  │  │
    │   │   ├──────────┤      │BY_AREA       │     └──────┬──────┘  │  │
    │   │   │PARKING   │      └──────────────┘            │         │  │
    │   │   │_RAW      │                                  │         │  │
    │   │   ├──────────┤       DYNAMIC TABLES             │         │  │
    │   │   │CHARGING  │       (auto-refresh)             │         │  │
    │   │   │_RAW      │                                  │         │  │
    │   │   └──────────┘                                  │         │  │
    │   └─────────────────────────────────────────────────┼─────────┘  │
    │                                                     │            │
    │                  ┌──────────────────────────────────┼────────┐   │
    │                  │                                  │        │   │
    │            ┌─────▼─────┐                      ┌─────▼─────┐  │   │
    │            │ STREAMLIT │                      │  SECURE   │  │   │
    │            │ DASHBOARD │                      │  DATA     │  │   │
    │            │           │                      │  SHARE    │  │   │
    │            │ EV Growth │                      │           │  │   │
    │            │ Fuel Mix  │                      │ Dealers   │  │   │
    │            │ Charging  │                      │ Partners  │  │   │
    │            │ Market    │                      │ OEMs      │  │   │
    │            │ Insights  │                      │           │  │   │
    │            └───────────┘                      └───────────┘  │   │
    │                                                              │   │
    │   ┌──────────────────────────────────────────────────────────┘   │
    │   │ PON_ANALYTICS_WH                                             │
    │   │ • Multi-cluster (1-3)  • Auto-suspend 60s                    │
    │   │ • Resource Monitor     • Per-second billing                  │
    │   └──────────────────────────────────────────────────────────────┘
    └────────────────────────────────────────────────────────────────────┘
```

## Why Snowflake?

Pon's pain points and how Snowflake addresses them:

| Pain Point | Current State | Snowflake Solution |
|------------|--------------|-------------------|
| **6-hour query times** | Slow processing | Instant elastic scaling |
| **Manual scaling** | Cluster management | Zero infrastructure management |
| **Concurrency issues** | Sessions getting closed | Multi-cluster auto-scale |
| **High maintenance costs** | Complex pipelines | Dynamic Tables (no orchestration) |
| **Data silos** | Copies everywhere | Secure Data Sharing (live, no copies) |

### Platform Comparison

| Capability | Snowflake | Databricks | Microsoft Fabric |
|------------|-----------|------------|------------------|
| **Cluster Management** | None (fully serverless) | Serverless SQL available; classic clusters require config | Capacity units to manage |
| **Startup Time** | Instant (<1s) | Serverless: ~15s; Classic: 2-5 min | Depends on capacity mode |
| **Concurrent Users** | Auto-scale out (transparent) | Auto-scale available | Shared capacity model |
| **Declarative Pipelines** | Dynamic Tables (SQL-native) | Delta Live Tables (Python/SQL) | Dataflows (Power Query) |
| **Cost Control** | Resource Monitors (hard limits) | Budgets + DBU tracking | Capacity allocation |
| **Cross-Org Data Sharing** | Native zero-copy (any cloud) | Delta Sharing (Unity Catalog required) | External sharing (limited) |
| **Embedded BI** | Streamlit (native) | Dashboards (native) | Power BI (native) |

## How This Lab Maps to Pon's Requirements

| Pon's Criteria | Lab Module | How We Address It |
|----------------|------------|-------------------|
| **Business Question**: "Which region has fastest EV growth and does it correlate with charging?" | Module 7 | Dashboard Tab 2 shows EV adoption vs charging infrastructure by region with explicit answer |
| **RDW Open Data APIs** with pagination (1000 row limit) | Module 2 | Python UDF with External Access handles `$limit` and `$offset` pattern |
| **Target Model: brandst per postcode per datum** | Module 3 | Dynamic Table `BRANDSTOF_PER_POSTCODE_DATUM` with Postcode, Datum, Brandstof, Aantal |
| **Target Model: Laadpalen per postcode** | Module 3 | Dynamic Table `LAADPALEN_PER_POSTCODE` with Postcode, Aantal |
| **RAW → CURATED → ANALYTICS** pipeline | Module 3 | Three-schema medallion architecture with Dynamic Tables |
| **No manual orchestration** | Module 3 | Dynamic Tables with `TARGET_LAG` — no external scheduler needed |
| **Cost control and scaling** | Module 4 | Multi-cluster warehouse + Resource Monitor with hard limits |
| **Share data with dealers** (no copies) | Module 5 | Secure Data Sharing with zero-copy live access |
| **Single platform** (no external tools) | All | Everything runs in Snowflake — ingestion, pipelines, dashboards |

## Features Covered

| Feature | Description | Module |
|---------|-------------|--------|
| **External Access Integration** | Securely call external APIs from Snowflake | 2 |
| **Python UDFs** | Custom functions for API pagination | 2 |
| **LATERAL FLATTEN** | Efficient JSON array processing | 2 |
| **Dynamic Tables** | Declarative pipelines with automatic refresh | 3 |
| **Multi-Cluster Warehouses** | Auto-scale for concurrent workloads | 4 |
| **Resource Monitors** | Automatic cost control and alerts | 4 |
| **Secure Data Sharing** | Zero-copy sharing across organizations | 5 |
| **Snowflake Marketplace** | Instant access to third-party data | 6 |
| **Streamlit in Snowflake** | Native dashboards, no external hosting | 7 |

## Data Sources (per PDF Requirements)

All data comes from **RDW Open Data** (Dutch Vehicle Authority) - no synthetic data:

| Dataset | RDW ID | Required Columns (PDF) | Records |
|---------|--------|------------------------|---------|
| **Gekentekende_voertuigen** | `m9d7-ebf2` | Kenteken, datum_eerste_tenaamstelling | 50,000 |
| **Gekentekende_voertuigen_brandstof** | `8ys7-d773` | Kenteken, brandstof_omschrijving | 150,000 |
| **Voertuigen per postcode** | `8wbe-pu7d` | Postcode, Brandstof, Aantal | 46,645 |
| **Parkeeradres** | `ygq4-hh5q` | parkingaddressreference, zipcode (filter: parkingaddresstype='F') | 3,382 |
| **SPECIFICATIES PARKEERGEBIED** | `b3us-f26s` | areamanagerid, chargingpointcapacity | 3,139 |

### Data Loading Strategy

> **Critical for RFP evaluation:** The datasets below are intentionally loaded at different coverage levels.

| Dataset | PDF Volume | Lab Load | Coverage | Purpose |
|---------|-----------|----------|----------|---------|
| **Voertuigen per postcode** | 46,644 | 46,645 | **100%** | Primary source for regional EV analysis |
| **SPECIFICATIES PARKEERGEBIED** | 3,139 | 3,139 | **100%** | Required for laadpalen (charging) correlation |
| **Parkeeradres** | 3,792 | 3,382 | **89%** | Filtered per PDF: `parkingaddresstype='F'` |
| **Gekentekende_voertuigen** | 16.7M | 50K | 0.3% | Sampled for time-series (optional analysis) |
| **Gekentekende_voertuigen_brandstof** | 16.7M | 150K | 0.9% | Sampled for fuel trends (optional analysis) |

**Why this approach:**

1. **The core business question** from the PDF (*"Welke regio heeft de snelste groei van EV's en zie je dat terug in aantal beschikbare laadpalen?"*) requires:
   - Vehicles by postcode → **100% loaded**
   - Charging infrastructure → **100% loaded**
   
2. **The two sampled datasets** (16.7M rows each) are used only for supplementary time-series analysis, not the core deliverable.

3. **Loading full 16.7M rows** would require ~2 hours and additional compute credits. For production, simply increase the `ROWCOUNT` parameters in the ingestion scripts.

### Data Model: Laadpalen per Postcode

To create the target model "Laadpalen per postcode" (Postcode, Aantal), join the two parking/charging datasets:

```
Parkeeradres.parkingaddressreference = SPECIFICATIES.areamanagerid
```

Then aggregate `chargingpointcapacity` by `zipcode` from Parkeeradres.

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** access (trial accounts work)
- Web browser (everything runs inside Snowflake)

## Lab Agenda

| Module | Topic | Duration |
|--------|-------|----------|
| 0 | Environment Setup | 10 min |
| 1 | Database & Schema Design | 10 min |
| 2 | API Data Ingestion | 25 min |
| 3 | Dynamic Tables Pipeline | 20 min |
| 4 | Scaling & Cost Control | 15 min |
| 5 | Secure Data Sharing | 15 min |
| 6 | Marketplace Data Enrichment | 15 min |
| 7 | Streamlit Dashboard | 15 min |
| 8 | Wrap-up & Discussion | 10 min |

**Total: ~2.5 hours**

## Getting Started

1. Open the **[Lab Guide](lab_guide.md)** for the step-by-step walkthrough
2. Start at **Module 0** to set up your environment
3. Follow each module in order

## Repository Contents

```
pon-automotive-lab/
├── README.md                 ← You are here
├── lab_guide.md              ← Full step-by-step lab guide (start here)
├── assets/
│   ├── banner.svg            ← GitHub banner
│   └── divider.svg           ← Section divider
├── scripts/
│   ├── 00_complete_setup.sql ← All SQL in one file (facilitator backup)
│   ├── 01_setup.sql          ← Module 1: Database and schemas
│   ├── 02_data_ingestion.sql ← Module 2: External access and UDFs
│   ├── 03_dynamic_tables.sql ← Module 3: Automated pipelines
│   ├── 04_scaling_cost.sql   ← Module 4: Warehouses and monitors
│   ├── 05_data_sharing.sql   ← Module 5: Secure sharing
│   └── 06_marketplace.sql    ← Module 6: Marketplace data queries
└── streamlit_app.py          ← Module 7: Dashboard code (copy to Snowsight)
```

## About This Lab

Built for **Pon Automotive** to demonstrate how Snowflake powers strategic EV planning. Real RDW data answers the core question: *Which region in the Netherlands has the fastest EV growth?*

**Key differentiators:**
- Zero infrastructure management (no clusters to configure)
- Automatic pipelines (Dynamic Tables replace orchestration)
- Instant scalability (warehouses scale in seconds)
- Native data sharing (share live data with dealers, no copies)
- Unified platform (data engineering to dashboards in one place)

---

<p align="center">
  <img src="https://img.shields.io/badge/Built_for-Pon_Automotive-FF6B00?style=flat-square" alt="Built for Pon Automotive">
  <img src="https://img.shields.io/badge/Powered_by-Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white" alt="Powered by Snowflake">
</p>
