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

## The Story: Why This Lab Exists

**Pon Automotive** manages one of the largest vehicle portfolios in the Netherlands. Like every automotive company in Europe, the shift to electric vehicles is reshaping the business — from which models to stock, to where charging partnerships make sense, to how to advise dealers on regional inventory.

Today, answering these questions means pulling data from DB2, running manual exports, waiting for overnight batch jobs, and assembling spreadsheets. By the time the analysis is ready, the data is already stale. Dealers get CSV exports via email — ungoverned copies that drift out of sync within days.

**The rest of the Pon Group is already on Snowflake.** Pon Automotive is the last major division to modernize its data platform. This creates a unique opportunity: by choosing Snowflake, Pon Automotive can share live data across the entire Pon Group — dealers, OEM partners, and sister companies — without building integration pipelines. The data is simply *there*, governed and current.

This lab puts that vision into practice. You will build a complete data engineering solution — from live government API ingestion to an interactive dashboard — entirely within a single platform. No external schedulers, no cluster management, no separate BI tools.

### The Business Question

> *"Which region in the Netherlands has the fastest EV growth, and does that correlate with charging infrastructure availability?"*

This drives real decisions: dealership inventory allocation, charging station partnerships, regional marketing strategy, and fleet transition planning.

### The Answer (Spoiler)

By the end of this lab, your dashboard will show:

| Insight | Finding |
|---------|---------|
| **Highest EV adoption** | Amsterdam area leads with ~25% EV share |
| **Biggest infrastructure gap** | Regions with 500+ EVs per charging location |
| **Correlation** | High EV adoption does not guarantee sufficient charging — expansion opportunities identified |

This is real RDW data, analyzed in real-time via Dynamic Tables, visualized in Streamlit.

## Where You Are Today vs Where This Lab Takes You

| Challenge | Today (DB2 / Manual) | After This Lab |
|-----------|---------------------|----------------|
| **Getting RDW data** | Manual CSV exports, FTP transfers, days of delay | Live API calls from inside the platform — data arrives in minutes |
| **Keeping models fresh** | Overnight batch jobs, stale by morning | Dynamic Tables refresh automatically — you set a freshness target, the platform handles the rest |
| **Handling 50 concurrent users** | Sessions time out, 6-hour query waits | Compute scales out transparently — every user gets dedicated resources |
| **Sharing data with dealers** | Email CSV attachments, ungoverned copies | Zero-copy live sharing — dealers see current data, access revoked in seconds |
| **Adding external context** | Procurement, contracts, ETL pipelines | Marketplace datasets appear instantly, no data movement |
| **Building dashboards** | Separate BI tool, data extracts, sync issues | Dashboard runs inside the platform, queries live data directly |
| **Controlling cloud costs** | Unpredictable bills, no hard limits | Resource monitors with automatic suspension — budget exceeded means compute stops, not a surprise invoice |

## The Pon Group Advantage

**This is the most important consideration in this evaluation.**

The rest of the Pon Group already runs on Snowflake. Choosing Snowflake for Pon Automotive means:

- **Instant data sharing across divisions** — no integration projects, no ETL, no copies. Pon Automotive's EV analytics become available to the entire group the moment you grant access.
- **Unified governance** — one set of access policies, one audit trail, one security model across the entire Pon data estate.
- **Shared Marketplace subscriptions** — weather data, demographics, and emissions data acquired once, available to every Pon division.
- **Common skills and tooling** — engineers can move between Pon divisions without retraining. SQL works the same everywhere.

With an alternative platform, Pon Automotive would need to build and maintain cross-platform integration pipelines to share data with the rest of the group — an ongoing cost that grows with every new data product.

> **Ask every vendor in this evaluation:** *"How would Pon Automotive share live, governed data with the rest of the Pon Group, which runs on Snowflake, without building ETL pipelines?"*

## What You Will Build

A complete data engineering solution using **real Dutch government data**, all within a single platform:

1. **Live API Data Ingestion** — Real-time data from RDW (Dutch Vehicle Authority), fetched directly from SQL
2. **Automated Data Pipelines** — Dynamic Tables that refresh automatically, no external scheduler
3. **Cost-Controlled Analytics** — Multi-cluster warehouses with hard budget limits
4. **Cross-Organization Data Sharing** — Live, governed access for the dealer network (no copies)
5. **Marketplace Enrichment** — Weather and emissions data added instantly, no ETL
6. **Interactive Dashboard** — Streamlit app that answers the business question directly

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

## How This Lab Maps to the Use Case Requirements

Every requirement from the *EV transitie NL* use case document is addressed:

| Pon's Requirement | Lab Module | How It's Delivered |
|-------------------|------------|-------------------|
| **Core question**: "Welke regio heeft de snelste groei van EV's en zie je dat terug in aantal beschikbare laadpalen?" | Module 7 | Dashboard Tab 2 shows EV adoption vs charging infrastructure by region |
| **5 RDW Open Data APIs** with pagination (1000 row limit) | Module 2 | Python UDF with External Access handles `$limit` and `$offset` natively |
| **Target Model: brandst per postcode per datum** | Module 3 | Dynamic Table `BRANDSTOF_PER_POSTCODE_DATUM` (Datum, Brandstof, Aantal) |
| **Target Model: Laadpalen per postcode** | Module 3 | Dynamic Table `LAADPALEN_PER_POSTCODE` (Postcode, Aantal) |
| **RAW → CURATED → ANALYTICS pipeline** | Module 3 | Three-schema medallion architecture with automatic refresh |
| **No manual orchestration** | Module 3 | Dynamic Tables with `TARGET_LAG` — no external scheduler needed |
| **Cost control and scaling** | Module 4 | Multi-cluster warehouse + Resource Monitor with hard limits |
| **Share data with dealers** (no copies) | Module 5 | Zero-copy live access, revocable in seconds |
| **Single platform** (no external tools) | All | Ingestion, pipelines, dashboards, sharing — one platform |

## Questions to Ask Every Vendor

When evaluating platforms, these questions reveal real architectural differences:

| Question | Why It Matters for Pon |
|----------|----------------------|
| **"Can we call the RDW API directly from a SQL query, without a separate ingestion tool?"** | Pon needs to ingest from 5+ government APIs. Fewer moving parts means less maintenance. |
| **"How do we keep downstream models fresh without an external orchestrator?"** | Overnight batch staleness is a key pain point. Declarative freshness targets eliminate scheduler complexity. |
| **"If 50 dealers run reports at 9am Monday, does anyone wait?"** | Concurrency under load is where DB2 fails today. The scaling model matters. |
| **"How does Pon Automotive share live data with the rest of the Pon Group without building integration pipelines?"** | The Pon Group is already on Snowflake. Cross-division data sharing is the highest-value capability. |
| **"What happens when we exceed our compute budget — do we get a bill or does compute stop?"** | Moving from predictable on-prem costs to cloud consumption requires hard guardrails, not dashboards. |
| **"Can we enrich our vehicle data with weather and emissions data without building an ETL pipeline?"** | Third-party data enrichment should be instant, not a project. |

## Data Sources

All data comes from **RDW Open Data** (Dutch Vehicle Authority) — real government data, not synthetic:

| Dataset | RDW ID | Required Columns | Records |
|---------|--------|-----------------|---------|
| **Gekentekende_voertuigen** | `m9d7-ebf2` | Kenteken, datum_eerste_tenaamstelling | 50,000 |
| **Gekentekende_voertuigen_brandstof** | `8ys7-d773` | Kenteken, brandstof_omschrijving | 50,000 |
| **Voertuigen per postcode** | `8wbe-pu7d` | Postcode, Brandstof, Aantal | 46,645 |
| **Parkeeradres** | `ygq4-hh5q` | parkingaddressreference, zipcode | 3,382 |
| **SPECIFICATIES PARKEERGEBIED** | `b3us-f26s` | areamanagerid, chargingpointcapacity | 3,139 |

The core datasets (vehicles by postcode, parking, charging) are loaded at **100% coverage**. The two large datasets (16.7M rows each) are sampled at 50K for the lab — increase `ROWCOUNT` for production.

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** access — a [free trial](https://signup.snowflake.com/) works perfectly (30 days, $400 credits). **Select Enterprise edition** during signup to get multi-cluster warehouses in Module 4. If you pick Standard, everything works except that one step.
- Web browser (everything runs inside Snowflake — no local tooling required)

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
| **Bonus** | **AI-Assisted Development** | 15 min |

**Total: ~2.5 hours** (+ optional bonus)

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
│   ├── 00_complete_setup.sql ← All SQL in one file (backup)
│   ├── 01_setup.sql          ← Module 1: Database and schemas
│   ├── 02_data_ingestion.sql ← Module 2: External access and UDFs
│   ├── 03_dynamic_tables.sql ← Module 3: Automated pipelines
│   ├── 04_scaling_cost.sql   ← Module 4: Warehouses and monitors
│   ├── 05_data_sharing.sql   ← Module 5: Secure sharing
│   ├── 06_marketplace.sql    ← Module 6: Marketplace data queries
│   ├── 06_bonus_cortex_code.sql ← Bonus: AI-assisted pipeline development
│   ├── 07_demo_governance.sql   ← Demo: Governance features
│   └── 08_reader_account_setup.sql ← Demo: Reader account setup
└── streamlit_app.py          ← Module 7: Dashboard code
```

## What This Lab Demonstrates

This lab was built for the Pon Automotive data platform evaluation. It answers the use case requirements with real data, real pipelines, and a real dashboard — all running on a single platform with zero external tooling.

**What makes this approach different:**
- **Zero infrastructure management** — no clusters to configure, no servers to patch, no capacity to pre-provision
- **Automatic pipelines** — declare what you want, not how to schedule it
- **Instant scalability** — compute scales in seconds, not minutes
- **Native data sharing** — live data flows to dealers and the Pon Group without building integrations
- **Unified platform** — data engineering, analytics, and dashboards in one place with one governance model

---

<p align="center">
  <img src="https://img.shields.io/badge/Built_for-Pon_Automotive-FF6B00?style=flat-square" alt="Built for Pon Automotive">
  <img src="https://img.shields.io/badge/Powered_by-Snowflake-29B5E8?style=flat-square&logo=snowflake&logoColor=white" alt="Powered by Snowflake">
</p>
