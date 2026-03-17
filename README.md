<p align="center">
  <img src="assets/banner.svg" alt="Pon Automotive EV Transition Lab" width="100%">
</p>

<h1 align="center">Pon Automotive — EV Transition Netherlands</h1>

<p align="center">
  <b>Hands-on: Building Scalable Data Pipelines for Electric Vehicle Analytics</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Duration-2_hours-FF6B00?style=for-the-badge" alt="Duration: 2 hours">
  <img src="https://img.shields.io/badge/Level-Intermediate-0069B4?style=for-the-badge" alt="Level: Intermediate">
  <img src="https://img.shields.io/badge/Platform-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Platform: Snowflake">
</p>

<p align="center"><img src="assets/divider.svg" width="80%"></p>

## The Business Challenge

**Pon Automotive** manages one of the largest vehicle portfolios in the Netherlands. With the accelerating shift to electric vehicles, leadership needs to answer a critical question:

> *"Which region in the Netherlands has the fastest EV growth, and does that correlate with charging infrastructure availability?"*

This insight drives strategic decisions about:
- Dealership inventory allocation
- Charging station partnerships
- Regional marketing campaigns
- Fleet transition planning

## What You Will Build

In this lab you will create an end-to-end data engineering solution using **real Dutch government data** — entirely within Snowflake. No external tools, no complex orchestration, no cluster management.

By the end you will have:

1. **Live API Data Ingestion** — Real-time data from RDW (Dutch Vehicle Authority) using External Access Integration
2. **Automated Data Pipelines** — Dynamic Tables that refresh automatically with zero orchestration
3. **Cost-Controlled Analytics** — Multi-cluster warehouses with resource monitors
4. **Cross-Organization Data Sharing** — Secure sharing with dealers (no data copies!)
5. **Interactive Dashboard** — Streamlit in Snowflake for EV growth visualization

### Architecture Overview

```
                         SNOWFLAKE ACCOUNT
    ┌──────────────────────────────────────────────────────────┐
    │                                                          │
    │   EXTERNAL                                               │
    │   ┌─────────┐     ┌──────────────────────────────────┐  │
    │   │ RDW     │     │           PON_EV_LAB             │  │
    │   │ Open    │     │                                  │  │
    │   │ Data    │────▶│  RAW          CURATED    ANALYTICS│  │
    │   │ APIs    │     │  ┌─────┐     ┌─────┐    ┌─────┐ │  │
    │   └─────────┘     │  │VEHIC│     │VEHIC│    │EV   │ │  │
    │                   │  │LES  │────▶│LES_ │───▶│GROWT│ │  │
    │   External        │  │     │     │WITH │    │H_   │ │  │
    │   Access          │  │FUEL │     │FUEL │    │TREND│ │  │
    │   Integration     │  │     │     │     │    │S    │ │  │
    │                   │  │PARKI│     │CHARG│    │     │ │  │
    │   Python UDF      │  │NG   │────▶│ING_ │    │YOY_ │ │  │
    │   (paginated      │  │     │     │BY_  │    │GROWT│ │  │
    │    fetch)         │  │CHARG│     │AREA │    │H    │ │  │
    │                   │  │ING  │     │     │    │     │ │  │
    │                   │  └─────┘     └─────┘    └──┬──┘ │  │
    │                   │                           │     │  │
    │                   │              DYNAMIC TABLES      │  │
    │                   │            (auto-refresh!)       │  │
    │                   └──────────────────────────────────┘  │
    │                                      │                  │
    │                    ┌─────────────────┴──────────────┐   │
    │                    │                                │   │
    │              ┌─────▼─────┐                ┌─────────▼─┐ │
    │              │ STREAMLIT │                │   SECURE  │ │
    │              │ DASHBOARD │                │   DATA    │ │
    │              │           │                │   SHARE   │ │
    │              │ EV Growth │                │           │ │
    │              │ Fuel Mix  │                │ Dealers   │ │
    │              │ Charging  │                │ Partners  │ │
    │              │ Infra     │                │ OEMs      │ │
    │              └───────────┘                └───────────┘ │
    │                                                         │
    │   ┌─────────────────────────────────────────────────┐   │
    │   │ PON_ANALYTICS_WH                                │   │
    │   │ • Multi-cluster (1-3)  • Auto-suspend 60s      │   │
    │   │ • Resource Monitor     • Per-second billing    │   │
    │   └─────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────┘
```

## Why Snowflake? (vs. Databricks & Fabric)

Pon's current pain points and how Snowflake solves them:

| Pain Point | Current State | Snowflake Solution |
|------------|--------------|-------------------|
| **6-hour query times** | Slow processing | Instant elastic scaling |
| **Manual scaling** | Cluster management | Zero infrastructure management |
| **Concurrency issues** | Sessions getting closed | Multi-cluster auto-scale |
| **High maintenance costs** | Complex pipelines | Dynamic Tables (no orchestration) |
| **Data silos** | Copies everywhere | Secure Data Sharing (live, no copies) |

### Head-to-Head Comparison

| Capability | Snowflake | Databricks | Microsoft Fabric |
|------------|-----------|------------|------------------|
| **Cluster Management** | None (serverless) | Manual cluster config | Capacity units |
| **Startup Time** | Instant | 2-5 minutes | Variable |
| **Concurrent Users** | Auto-scale out | Manual scaling | Shared capacity (throttling) |
| **Pipeline Orchestration** | Dynamic Tables | Requires Workflows/Airflow | Requires Data Factory |
| **Cost Control** | Resource Monitors | DBU tracking (complex) | Capacity allocation |
| **Cross-Org Data Sharing** | Native (zero-copy) | Delta Sharing (separate) | Not available |
| **Embedded BI** | Streamlit native | Separate deployment | Power BI (separate) |

## Snowflake Features Covered

| Feature | Description | Module |
|---------|-------------|--------|
| **External Access Integration** | Securely call external APIs from Snowflake | 2 |
| **Python UDFs** | Custom functions for API pagination | 2 |
| **LATERAL FLATTEN** | Efficient JSON array processing | 2 |
| **Dynamic Tables** | Declarative pipelines with automatic refresh | 3 |
| **Multi-Cluster Warehouses** | Auto-scale for concurrent workloads | 4 |
| **Resource Monitors** | Automatic cost control and alerts | 4 |
| **Secure Data Sharing** | Zero-copy sharing across organizations | 5 |
| **Streamlit in Snowflake** | Native dashboards, no external hosting | 6 |

## Data Sources

All data comes from **RDW Open Data** (Dutch Vehicle Authority) — the same datasets specified in the project requirements:

| Dataset | RDW ID | Description | Records |
|---------|--------|-------------|---------|
| Gekentekende Voertuigen | `m9d7-ebf2` | Registered vehicles | 50,000+ |
| Brandstof | `8ys7-d773` | Fuel types per vehicle | 100,000+ |
| Parkeerlocaties | `ygq4-hh5q` | Parking locations | 3,000+ |
| Laadpalen Capaciteit | `b3us-f26s` | Charging point capacity | 3,000+ |

## Prerequisites

- A Snowflake account with **ACCOUNTADMIN** access (trial accounts work!)
- A web browser — everything runs inside Snowflake

## Lab Agenda

| Module | Topic | Duration |
|--------|-------|----------|
| 0 | Environment Setup | 10 min |
| 1 | Database & Schema Design | 10 min |
| 2 | API Data Ingestion | 25 min |
| 3 | Dynamic Tables Pipeline | 20 min |
| 4 | Scaling & Cost Control | 15 min |
| 5 | Secure Data Sharing | 15 min |
| 6 | Streamlit Dashboard | 15 min |
| 7 | Wrap-up & Discussion | 10 min |

**Total: ~2 hours**

## Getting Started

1. Open the **[Lab Guide](lab_guide.md)** — this is the step-by-step walkthrough
2. Start at **Module 0** to set up your environment
3. Follow each module in order — every step explains *what* and *why*

## Repository Contents

```
pon-automotive-lab/
├── README.md                 ← You are here
├── lab_guide.md              ← Full step-by-step lab guide (start here!)
├── assets/
│   ├── banner.svg            ← GitHub banner (Pon branded)
│   └── divider.svg           ← Section divider
├── scripts/
│   ├── 00_complete_setup.sql ← All SQL in one file (facilitator backup)
│   ├── 01_setup.sql          ← Module 1: Database and schemas
│   ├── 02_data_ingestion.sql ← Module 2: External access and UDFs
│   ├── 03_dynamic_tables.sql ← Module 3: Automated pipelines
│   ├── 04_scaling_cost.sql   ← Module 4: Warehouses and monitors
│   └── 05_data_sharing.sql   ← Module 5: Secure sharing
├── facilitator/
│   └── setup_checklist.md    ← Pre-lab setup for facilitators
├── streamlit_app.py          ← Dashboard code
├── environment.yml           ← Conda dependencies for Streamlit
└── requirements.txt          ← Alternative pip dependencies
```

## About This Lab

This lab was designed specifically for **Pon Automotive** to demonstrate how Snowflake addresses their data engineering challenges. The use case, data sources, and architecture directly reflect the "EV Transitie NL" project requirements.

**Key differentiators demonstrated:**
- **Zero infrastructure management** — No clusters to configure or manage
- **Automatic pipelines** — Dynamic Tables replace complex orchestration
- **Instant scalability** — Warehouses scale in seconds, not minutes
- **Native data sharing** — Share live data with dealers without copies
- **Unified platform** — Data engineering to dashboards in one place

---

<p align="center">
  <img src="https://img.shields.io/badge/Built_for-Pon_Automotive-FF6B00?style=flat-square" alt="Built for Pon Automotive">
  <img src="https://img.shields.io/badge/Powered_by-Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white" alt="Powered by Snowflake">
</p>
