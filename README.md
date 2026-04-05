# Community Health Intelligence Platform

> A production-grade Databricks SQL Dashboard for Kenya's Community Health Worker (CHW) Program — delivering real-time analytics for immunization coverage, maternal health continuum, and CHW field operations.

![Databricks](https://img.shields.io/badge/Databricks-SQL-orange)
![Status](https://img.shields.io/badge/Status-Production-green)
![RLS](https://img.shields.io/badge/Row--Level%20Security-Enabled-blue)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Overview

This platform provides a 3-page executive dashboard that monitors community health outcomes across Kenyan counties (Busia & Kisumu), with row-level security ensuring each user sees only their authorized data.

### Key Metrics Tracked
| Domain | Metrics |
|--------|---------|
| **Immunization** | Penta3 Coverage (60.8%), Fully Immunized Children (5,874), Sub-county Rankings |
| **Maternal Health** | ANC1→ANC4+ Cascade (83.2%→25.7%), Defaulter Rate (16.3%), Skilled Delivery (48.1%), PNC1 (30.8%) |
| **CHW Operations** | Active CHWs (4,671), Household Visits (3.2M), Productivity Distribution, Zone Performance |
| **Family Planning** | Modern Contraceptive Prevalence Rate (mCPR: 3.6%) |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Dashboard Layer                       │
│  ┌─────────────┬──────────────┬───────────────────┐     │
│  │  Executive   │   Maternal   │   CHW Field       │     │
│  │  Command     │   Continuum  │   Operations      │     │
│  │  Center      │   of Care    │                   │     │
│  └──────┬──────┴──────┬───────┴────────┬──────────┘     │
│         │             │                │                 │
│  ┌──────▼─────────────▼────────────────▼──────────┐     │
│  │         Row-Level Security Functions            │     │
│  │   county_access_filter() + subcounty_access()   │     │
│  └──────┬─────────────┬────────────────┬──────────┘     │
├─────────┼─────────────┼────────────────┼────────────────┤
│         │        Gold Layer            │                 │
│  ┌──────▼──────┐ ┌────▼─────┐ ┌───────▼──────────┐     │
│  │ Dimensions  │ │  Facts   │ │ Materialized     │     │
│  │ dim_chw     │ │ fact_*   │ │ Views (mv_*)     │     │
│  │ dim_facility│ │ (7 tbls) │ │ + Views (vw_*)   │     │
│  │ dim_geo     │ │          │ │                  │     │
│  └─────────────┘ └──────────┘ └──────────────────┘     │
│                                                         │
│  Silver Layer → Bronze Layer → Source Systems            │
└─────────────────────────────────────────────────────────┘
```

## Dashboard Pages

### Page 1: Executive Command Center
- **6 KPI counters** — Penta3, Fully Immunized, ANC4+, Defaulter Rate, mCPR, Active CHWs
- **Immunization bar chart** — Coverage by county with sub-county breakdown
- **Sub-county rankings table** — Color-coded by coverage status (On Track / At Risk / Critical)
- **mCPR gauge** — Modern contraceptive prevalence with target line
- **Maternal care funnel** — ANC1→ANC4+→Delivery→PNC cascade

### Page 2: Maternal Continuum of Care
- **6 KPI counters** — ANC1, ANC2, ANC4+, Defaulter, Skilled Delivery, PNC1
- **Cascade funnel** — 6-stage visualization (ANC1→ANC2→ANC3→ANC4+→Delivery→PNC)
- **County comparison** — Busia vs Kisumu across all maternal metrics
- **Sub-county performance table** — 9 sub-counties with conditional status formatting
- **Monthly trend** — Dual-axis ANC4+ % and Defaulter Rate over time
- **Delta insight cards** — Month-over-month change indicators

### Page 3: CHW Field Operations
- **6 KPI counters** — Active CHWs, HH Visits, Avg Daily Visits, Beneficiaries, Defaulters, Below Target
- **Monthly visit trend** — Stacked bar by county
- **Activity feed** — Recent operational highlights
- **Zone performance table** — CHW count, IZ coverage %, status by zone
- **Clinical visit breakdown** — Pie chart (Immunization 80.8%, Defaulter 14.2%, ANC 5.0%)
- **Productivity distribution** — CHW productivity histogram

## Row-Level Security (RLS)

The platform implements fine-grained access control:

| Access Level | Scope | Example |
|---|---|---|
| **ADMIN** | All counties, all sub-counties | `keyegonaws@gmail.com` |
| **COUNTY** | All sub-counties in assigned county | `busia.manager@example.com` → BUSIA |
| **SUBCOUNTY** | Single sub-county only | `teso.north.officer@example.com` → TESO NORTH |

**Implementation:**
- `county_access_filter(county)` — SQL function with EXISTS() subquery against `user_access_control`
- `subcounty_access_filter(county, sub_county)` — Cascading function for sub-county level
- Applied as **row filters** on 11 gold tables; 8 views inherit RLS automatically

## Data Model

### Unity Catalog Structure
```
community_health_intelligence
├── bronze/    (raw ingested data)
├── silver/    (cleaned, standardized)
└── gold/      (analytics-ready)
    ├── Dimensions: dim_chw, dim_facility, dim_geography
    ├── Facts: fact_family_planning, fact_home_visit, fact_immunization,
    │          fact_pnc, fact_population, fact_pregnancy,
    │          fact_pregnancy_journey, fact_supervision
    ├── Materialized Views: mv_family_planning, mv_immunization,
    │                       mv_maternal_health, mv_supervision
    └── Views: vw_chw_performance, vw_coverage_gaps,
               vw_executive_summary, vw_maternal_funnel
```

### Key Columns
- `county_clean` / `sub_county_clean` — Standardized geography (UPPER CASE)
- `reportedm` — Reporting month (date)
- `date_key` — YYYYMM integer format for home visits
- `chw_area_uuid` — CHW identifier linking facts to dimensions

## Repository Structure

```
├── README.md
├── LICENSE
├── dashboards/
│   └── community_health_intelligence_platform.lvdash.json
├── notebooks/
│   ├── rls_setup.ipynb              # Row-Level Security setup & verification
│   ├── chw_rls_setup.ipynb          # CHW-specific RLS configuration
│   └── chw_semantic_model.ipynb     # Semantic model & data pipeline
├── sql/
│   ├── rls_functions.sql            # RLS function definitions
│   ├── gold_table_schemas.sql       # Gold layer DDL
│   └── sample_queries.sql           # Dashboard dataset queries
└── docs/
    ├── data_dictionary.md           # Column definitions & business logic
    └── rls_access_matrix.md         # User access control documentation
```

## Setup & Deployment

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless recommended)
- Catalog: `community_health_intelligence` with bronze/silver/gold schemas

### Quick Start
1. Clone this repo into a Databricks Git folder
2. Run `notebooks/chw_semantic_model.ipynb` to set up the data model
3. Run `notebooks/rls_setup.ipynb` to configure Row-Level Security
4. Import `dashboards/community_health_intelligence_platform.lvdash.json` as a dashboard
5. Publish the dashboard and configure warehouse credentials

### Global Filters
The dashboard supports 4 cascading filters:
- **County** — Filters all pages and datasets
- **Sub-County** — Cascades from County selection
- **Reporting Month** — Time period filter
- **Coverage Status** — On Track / At Risk / Critical

## Data Coverage

| Data Domain | Time Range | Grain |
|---|---|---|
| Home Visits | Dec 2024 – Apr 2025 | CHW × Month |
| Immunization | Jan 2025 – Mar 2025 | CHW × Month × Community Unit |
| Pregnancy/ANC | Jan 2025 – Feb 2025 | Individual pregnancy |
| Family Planning | Jan 2025 – Feb 2025 | Monthly summary |

## Known Data Quality Notes

- ~96K home visits (8.8%) map to UNKNOWN county due to 558 CHWs missing from dimension table
- Root cause: 91.5% are Busia/Kisumu records with CHW UUIDs not in master data
- 130 facilities have NULL county/sub-county mappings
- `mv_maternal_health` view is slow — dashboard queries bypass it using `fact_pregnancy` directly

## Technology Stack

- **Platform:** Databricks (AWS)
- **Storage:** Delta Lake on Unity Catalog
- **Dashboard:** AI/BI Dashboards (Lakeview)
- **Security:** Row-Level Security with SQL UDFs
- **Compute:** Serverless SQL Warehouse

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

## Author

**Erick Yegon**
- GitHub: [@erickyegon](https://github.com/erickyegon)
