<div align="center">

# 🏥 Community Health Intelligence Platform

### Real-Time Decision Intelligence for Kenya's Community Health Worker Program

[![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![Unity Catalog](https://img.shields.io/badge/Governance-Unity%20Catalog-4B0082?style=for-the-badge)](https://databricks.com/product/unity-catalog)
[![eCHIS](https://img.shields.io/badge/Source-Kenya%20eCHIS-008C45?style=for-the-badge)](https://echis.health.go.ke)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

**An end-to-end analytics platform transforming Kenya's national eCHIS community health records into actionable intelligence — from bronze ingestion through gold-layer analytics to executive dashboards with enterprise-grade row-level security and privacy-by-design data handling.**

[Explore the Dashboard](#-dashboard-pages) · [Data Source](#-data-source--echis) · [Architecture](#-architecture) · [Privacy & PII](#-privacy--pii-protection) · [Security](#-row-level-security) · [Getting Started](#-getting-started)

</div>

---

## 📋 Table of Contents

- [Executive Summary](#-executive-summary)
- [Data Source — eCHIS](#-data-source--echis)
- [Privacy & PII Protection](#-privacy--pii-protection)
- [Dashboard Pages](#-dashboard-pages)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Row-Level Security](#-row-level-security)
- [Performance Engineering](#-performance-engineering)
- [Data Quality Framework](#-data-quality-framework)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Technical Specifications](#-technical-specifications)

---

## 📊 Executive Summary

This platform serves as the **central nervous system** for Kenya's community health program, synthesizing data from **4,671 Community Health Workers** across **2 counties** and **17 sub-counties** into a unified decision-support system.

<table>
<tr>
<td width="33%" align="center">
<h3>3.2M+</h3>
<p>Household Visits Tracked</p>
</td>
<td width="33%" align="center">
<h3>339K</h3>
<p>Unique Beneficiaries</p>
</td>
<td width="33%" align="center">
<h3>26</h3>
<p>Analytical Datasets</p>
</td>
</tr>
<tr>
<td align="center">
<h3>19</h3>
<p>Gold-Layer Tables & Views</p>
</td>
<td align="center">
<h3>9</h3>
<p>RLS-Protected User Roles</p>
</td>
<td align="center">
<h3>5</h3>
<p>Months of Longitudinal Data</p>
</td>
</tr>
</table>

### The Challenge

County and sub-county health managers lacked a unified view of community health performance. Data lived in siloed systems, making it impossible to identify coverage gaps, track the maternal care continuum, or monitor CHW productivity at scale. Decision-makers needed **role-appropriate, real-time analytics** without compromising data governance or patient privacy.

### The Solution

A **three-tier analytics platform** built on Databricks Lakehouse architecture that:

- **Ingests** raw community health records from Kenya's national **eCHIS platform** through a medallion pipeline (Bronze → Silver → Gold)
- **Anonymizes** personally identifiable information at the silver layer, ensuring no patient-level PII reaches the analytics tier
- **Models** complex health domain relationships across immunization, maternal care, family planning, and CHW operations
- **Secures** data with function-based row-level security ensuring each user sees only their authorized geographic scope
- **Delivers** insights through a 3-page executive dashboard with cascading drill-down filters

---

## 🏛 Data Source — eCHIS

### About the Electronic Community Health Information System

This platform ingests data from Kenya's **National Electronic Community Health Information System (eCHIS)**, the country's official digital platform for community-level health service delivery.

| Attribute | Detail |
|-----------|--------|
| **Full Name** | Electronic Community Health Information System (eCHIS) |
| **Governing Body** | Kenya Ministry of Health, Division of Community Health Services |
| **Platform URL** | [https://echis.health.go.ke](https://echis.health.go.ke) |
| **Access** | Publicly accessible with authorized credentials (county/sub-county health teams) |
| **Coverage** | All 47 counties in Kenya (this project focuses on **Busia** and **Kisumu** counties) |
| **Data Domains** | Household registration, home visits, immunization, antenatal care, postnatal care, family planning, referrals, community-based surveillance |
| **Reporting Cadence** | Monthly aggregation from daily CHW mobile submissions |

### eCHIS Data Flow

```mermaid
flowchart LR
    subgraph eCHIS["<b>Kenya National eCHIS Platform</b>"]
        CHW["CHW Mobile App\n<i>Daily field data entry</i>"] --> Central["eCHIS Central\n<i>echis.health.go.ke</i>"]
    end

    subgraph Extract["<b>Data Extraction</b>"]
        Central --> |"Authorized\nAPI/Export"| Raw["Raw CSV/JSON\nExtracts"]
    end

    subgraph Platform["<b>This Platform</b>"]
        Raw --> |"Ingestion"| Bronze["Bronze\n<i>Raw records</i>"]
        Bronze --> |"PII removal\nStandardization"| Silver["Silver\n<i>De-identified</i>"]
        Silver --> |"Star schema\nAggregation"| Gold["Gold\n<i>Analytics-ready</i>"]
    end

    style eCHIS fill:#008C45,color:#fff
    style Extract fill:#333,color:#fff
    style Platform fill:#FF3621,color:#fff
```

### Data Scope

| Domain | eCHIS Module | Records | Time Range |
|--------|-------------|---------|------------|
| **Home Visits** | Household Visit Register | 3.2M visits | Dec 2024 – Apr 2025 |
| **Immunization** | Child Immunization Tracker | 5,874 fully immunized | Jan 2025 – Mar 2025 |
| **Maternal Health** | ANC/PNC Register | Pregnancy registrations | Jan 2025 – Feb 2025 |
| **Family Planning** | FP Service Delivery | FP client records | Jan 2025 – Feb 2025 |
| **CHW Workforce** | CHW Master Register | 4,671 active CHWs | Current |
| **Supervision** | CHW Supervision Checklist | Supervision assessments | Jan 2025 – Feb 2025 |

> **Note:** Raw eCHIS data is not included in this repository. Only SQL definitions, schema DDL, and dashboard configurations are versioned here. Access to source data requires authorized credentials from the respective County Health Management Team (CHMT).

---

## 🛡 Privacy & PII Protection

### Privacy-by-Design Framework

This platform implements a **multi-layered privacy architecture** aligned with Kenya's Data Protection Act (2019) and international health data handling standards.

```mermaid
flowchart TD
    subgraph Bronze["<b>Bronze Layer</b> — Raw Ingestion"]
        B1["Raw eCHIS extracts\n<i>May contain PII fields</i>"]
        style Bronze fill:#CD7F32,color:#fff
    end

    subgraph Silver["<b>Silver Layer</b> — De-identification"]
        S1["UUID pseudonymization\n<i>Patient IDs → opaque UUIDs</i>"]
        S2["Name field removal\n<i>Patient/beneficiary names dropped</i>"]
        S3["Geographic generalization\n<i>GPS → sub-county level only</i>"]
        S4["Date coarsening\n<i>Exact dates → reporting month</i>"]
        style Silver fill:#C0C0C0,color:#000
    end

    subgraph Gold["<b>Gold Layer</b> — Aggregate Analytics"]
        G1["No individual-level records\n<i>All metrics are aggregated</i>"]
        G2["Statistical disclosure control\n<i>Small cell suppression</i>"]
        style Gold fill:#FFD700,color:#000
    end

    subgraph Access["<b>Access Controls</b>"]
        A1["Row-Level Security\n<i>Geographic scoping per user</i>"]
        A2["Column-Level Governance\n<i>Sensitive fields excluded from views</i>"]
        A3["Audit Logging\n<i>All queries tracked</i>"]
        style Access fill:#4B0082,color:#fff
    end

    Bronze --> Silver --> Gold --> Access
```

### PII Handling by Pipeline Stage

| Stage | PII Treatment | Implementation |
|-------|--------------|----------------|
| **Bronze (Raw)** | Retained as-is for lineage | Access restricted to data engineers only; not exposed to dashboards or analysts |
| **Silver (Cleansed)** | **De-identified** | Patient names dropped; IDs replaced with `chw_area_uuid` pseudonyms; GPS coordinates removed; dates coarsened to month-level (`reportedm`) |
| **Gold (Analytics)** | **Aggregated** | All metrics computed at sub-county or CHW level — no individual patient records in the analytics layer |
| **Dashboard** | **Role-scoped** | Row-level security ensures users see only their authorized geographic area; no drill-down to individual beneficiaries |

### Data Protection Measures

| Measure | Description |
|---------|-------------|
| **Pseudonymization** | Patient and CHW identifiers are replaced with opaque UUIDs (`chw_area_uuid`, `chw_uuid`) that cannot be reversed without the bronze-layer mapping |
| **Minimization** | Only fields required for aggregate health analytics are promoted to the gold layer — names, phone numbers, precise addresses, and national IDs are excluded |
| **Geographic Generalization** | Location data is generalized to sub-county level; no household-level GPS coordinates appear in analytics tables |
| **Temporal Coarsening** | Individual encounter dates are aggregated to monthly reporting periods (`reportedm`, `date_key` as YYYYMM) |
| **Access Segregation** | Bronze layer access is restricted to pipeline service principals; analysts interact only with de-identified gold-layer tables |
| **Row-Level Security** | Unity Catalog row filters ensure county managers see only their county; sub-county officers see only their sub-county |
| **No Data in Repository** | This Git repository contains **zero data files** — only SQL definitions, schema DDL, and dashboard configurations. Raw data requires authorized eCHIS credentials. |
| **Audit Trail** | All data access is logged through Databricks Unity Catalog audit logs, providing full query-level traceability |

### Regulatory Alignment

| Framework | Compliance Approach |
|-----------|-------------------|
| **Kenya Data Protection Act (2019)** | De-identification at silver layer; purpose limitation (community health analytics only); data minimization in gold layer |
| **WHO Health Data Governance** | Aggregate reporting only; no individual-level health records in analytical outputs |
| **HIPAA Principles** (reference) | De-identification consistent with Safe Harbor method — 18 identifier categories addressed through removal or generalization |

> **⚠️ Important:** While this platform implements robust de-identification controls, deployers should conduct a formal **Data Protection Impact Assessment (DPIA)** per Kenya's Data Protection Act before processing live eCHIS data in production.

---

## 📸 Dashboard Pages

### Page 1: Executive Command Center

> Strategic overview for program leadership — key coverage indicators, geographic performance rankings, and cross-domain health metrics at a glance.

<p align="center">
  <img src="images/01_executive_command_center.png" alt="Executive Command Center" width="100%"/>
</p>

**Key Capabilities:**
- **6 real-time KPI counters** spanning immunization (Penta3: 60.8%), maternal health (ANC4+: 25.7%), and workforce metrics
- **Sub-county performance rankings** with conditional status classification (On Track / At Risk / Critical)
- **Cross-domain synthesis** — immunization coverage, mCPR gauge with target overlay, and maternal care funnel in a single view
- **County-level immunization comparison** with interactive drill-down

---

### Page 2: Maternal Continuum of Care

> End-to-end visibility into the maternal health cascade — from first antenatal contact through postnatal care, with dropout detection at every stage.

<p align="center">
  <img src="images/02_maternal_continuum_of_care.png" alt="Maternal Continuum of Care" width="100%"/>
</p>

**Key Capabilities:**
- **6-stage cascade funnel** (ANC1 → ANC2 → ANC3 → ANC4+ → Skilled Delivery → PNC) quantifying drop-off at each transition
- **County comparison analysis** — Busia vs. Kisumu across all maternal indicators revealing a **2.5× defaulter rate disparity**
- **Sub-county performance matrix** with status-based conditional formatting for rapid triage
- **Dual-axis monthly trend** tracking ANC4+ completion and defaulter rates over time
- **Delta insight cards** showing month-over-month directional changes

---

### Page 3: CHW Field Operations

> Operational intelligence for workforce management — productivity monitoring, visit pattern analysis, and zone-level performance tracking.

<p align="center">
  <img src="images/03_chw_field_operations.png" alt="CHW Field Operations" width="100%"/>
</p>

**Key Capabilities:**
- **CHW productivity distribution** with configurable threshold bins for performance segmentation
- **Clinical visit type breakdown** — isolating immunization (80.8%), defaulter follow-up (14.2%), and ANC visits (5.0%) from household visit noise
- **Monthly visit volume trends** with county-level color segmentation
- **Zone performance table** correlating CHW density with immunization coverage outcomes

---

## 🏗 Architecture

### Lakehouse Medallion Architecture

```mermaid
flowchart TB
    subgraph Sources["<b>Kenya National eCHIS</b>"]
        S1[("CHW Mobile\nApp Data")]
        S2[("Health Facility\nRegisters")]
        S3[("Population\nCensus Data")]
    end

    subgraph Bronze["<b>Bronze Layer</b> — Raw Ingestion"]
        B1["Raw CHW Reports\n<i>PII retained for lineage</i>"]
        B2["Raw Facility Data"]
        B3["Raw Population Data"]
    end

    subgraph Silver["<b>Silver Layer</b> — Cleansed & De-identified"]
        SV1["Geography Standardization\n<i>County/Sub-county normalization</i>"]
        SV2["PII Removal & Pseudonymization\n<i>Names dropped, IDs → UUIDs</i>"]
        SV3["Schema Enforcement\n<i>Type validation & coarsening</i>"]
    end

    subgraph Gold["<b>Gold Layer</b> — Analytics Ready"]
        direction TB
        subgraph Dimensions["Dimensions"]
            D1["dim_chw"]
            D2["dim_facility"]
            D3["dim_geography"]
        end
        subgraph Facts["Fact Tables"]
            F1["fact_immunization"]
            F2["fact_pregnancy"]
            F3["fact_home_visit"]
            F4["fact_family_planning"]
            F5["fact_pnc"]
            F6["fact_supervision"]
            F7["fact_population"]
            F8["fact_pregnancy_journey"]
        end
        subgraph Analytics["Materialized Views & Analytical Views"]
            MV1["mv_immunization"]
            MV2["mv_maternal_health"]
            MV3["mv_family_planning"]
            MV4["mv_supervision"]
            VW1["vw_executive_summary"]
            VW2["vw_maternal_funnel"]
            VW3["vw_chw_performance"]
            VW4["vw_coverage_gaps"]
        end
    end

    subgraph Security["<b>Row-Level Security</b>"]
        RLS["county_access_filter()\nsubcounty_access_filter()"]
        UAC[("user_access_control\n<i>9 role assignments</i>")]
    end

    subgraph Dashboard["<b>AI/BI Dashboard</b>"]
        P1["Executive\nCommand Center"]
        P2["Maternal\nContinuum"]
        P3["CHW Field\nOperations"]
    end

    Sources --> Bronze --> Silver --> Gold
    Gold --> Security --> Dashboard

    style Bronze fill:#CD7F32,color:#fff
    style Silver fill:#C0C0C0,color:#000
    style Gold fill:#FFD700,color:#000
    style Security fill:#4B0082,color:#fff
    style Dashboard fill:#006D5B,color:#fff
```

### Dashboard Data Flow

```mermaid
flowchart LR
    subgraph Filters["Global Cascading Filters"]
        CF["County"] --> SCF["Sub-County"]
        MF["Reporting Month"]
        CSF["Coverage Status"]
    end

    subgraph Datasets["26 Analytical Datasets"]
        direction TB
        ED["Executive\n5 datasets"]
        MD["Maternal\n10 datasets"]
        CD["CHW Ops\n3 datasets"]
        OD["Shared\n8 datasets"]
    end

    subgraph RLS["Row-Level Security"]
        RF["SQL UDF Filters\n<i>Evaluated per query</i>"]
    end

    Filters --> Datasets
    Datasets --> RLS
    RLS --> |"ADMIN: All Data"| A["Full Access"]
    RLS --> |"COUNTY: Filtered"| B["County Scope"]
    RLS --> |"SUBCOUNTY: Filtered"| C["Sub-County Scope"]
```

---

## 📐 Data Model

### Star Schema Design

```mermaid
erDiagram
    dim_chw ||--o{ fact_immunization : "chw_uuid"
    dim_chw ||--o{ fact_pregnancy : "chw_uuid"
    dim_chw ||--o{ fact_home_visit : "chw_area_uuid"
    dim_chw ||--o{ fact_family_planning : "chw_uuid"
    dim_chw ||--o{ fact_pnc : "chw_uuid"
    dim_chw ||--o{ fact_supervision : "chw_uuid"
    dim_geography ||--o{ fact_population : "county, sub_county"

    dim_chw {
        string chw_uuid PK "Pseudonymized identifier"
        string county_name "RLS-filtered"
        string sub_county_name "RLS-filtered"
        string community_unit
        string facility_name
    }

    dim_facility {
        string facility_id PK
        string facility_name
        string county_name "RLS-filtered"
        string sub_county_name "RLS-filtered"
        string facility_type
    }

    dim_geography {
        string county_name PK "RLS-filtered"
        string sub_county_name PK "RLS-filtered"
        string ward_name
    }

    fact_immunization {
        string chw_uuid FK "Pseudonymized"
        date reportedm "Month-level only"
        int penta1
        int penta3
        int is_fully_immunized
        int is_defaulter
    }

    fact_pregnancy {
        string chw_uuid FK "Pseudonymized"
        date reportedm "Month-level only"
        int anc_complete
        int is_anc_defaulter
        int has_danger_signs
    }

    fact_home_visit {
        string chw_area_uuid FK "Pseudonymized"
        int date_key "YYYYMM — no daily precision"
        bigint home_visits "Aggregate count"
        int active_days
        bigint unique_beneficiaries "Count only — no identifiers"
    }

    fact_family_planning {
        string chw_uuid FK "Pseudonymized"
        int is_on_fp
        int refilled_today
    }
```

### Metric Definitions

| Metric | Formula | Business Context |
|--------|---------|-----------------|
| **Penta3 Coverage** | `SUM(penta3) / COUNT(*)` | % of tracked children completing 3rd pentavalent dose |
| **ANC4+ Rate** | `SUM(anc_complete) / COUNT(*)` | % of pregnancies with ≥4 antenatal visits (WHO standard) |
| **Defaulter Rate** | `SUM(is_anc_defaulter) / COUNT(*)` | % of pregnancies missing scheduled ANC visits |
| **mCPR** | `SUM(on_fp) / SUM(wra_pop)` | Modern contraceptive prevalence among women of reproductive age |
| **Skilled Delivery** | `SUM(skilled_delivery) / COUNT(*)` | % of deliveries at health facilities with skilled attendants |
| **Avg Daily Visits** | `SUM(visits) / (SUM(active_months) × 22)` | Estimated daily household visits per CHW (22 working days/month) |

---

## 🔐 Row-Level Security

### Multi-Tier Access Control Architecture

The platform implements **function-based row-level security** through Unity Catalog, ensuring data governance without application-layer complexity.

```mermaid
flowchart TD
    User["Authenticated User"] --> Auth{"Check\nuser_access_control"}

    Auth --> |"ADMIN"| Admin["Full Dataset\n<i>All counties, all sub-counties</i>"]
    Auth --> |"COUNTY"| County["County-Scoped\n<i>All sub-counties in assigned county</i>"]
    Auth --> |"SUBCOUNTY"| SubCounty["Sub-County Scoped\n<i>Single sub-county only</i>"]

    Admin --> Query["SQL Query Execution"]
    County --> Filter1["county_access_filter()"] --> Query
    SubCounty --> Filter2["subcounty_access_filter()"] --> Query

    Query --> Gold[("Gold Layer\n11 tables filtered\n8 views inherit RLS")]

    style Admin fill:#10B981,color:#fff
    style County fill:#3B82F6,color:#fff
    style SubCounty fill:#F59E0B,color:#000
    style Gold fill:#FFD700,color:#000
```

### Security Implementation

| Component | Detail |
|-----------|--------|
| **Authentication** | Databricks workspace SSO via `CURRENT_USER()` |
| **Authorization** | `user_access_control` mapping table (9 roles) |
| **Enforcement** | SQL UDF row filters using `EXISTS()` subqueries |
| **Scope** | 11 gold tables with explicit row filters |
| **Inheritance** | 8 views automatically inherit parent table RLS |
| **Performance** | Filter pushdown — evaluated at storage layer, not application |

### Access Matrix

| Role | Scope | Tables Visible | Example Use Case |
|------|-------|---------------|-----------------|
| `ADMIN` | All data across all counties | 11 tables, unrestricted | National Program Director |
| `COUNTY` | All sub-counties within assigned county | 11 tables, county-filtered | County Health Management Team |
| `SUBCOUNTY` | Single sub-county only | 11 tables, sub-county-filtered | Sub-County Health Officer |

> **Design Decision:** RLS is implemented at the **storage layer** via Unity Catalog row filters rather than application-layer WHERE clauses. This ensures security is enforced regardless of access path — dashboard, notebook, SQL editor, or API — and is tamper-proof at the analyst level.

---

## ⚡ Performance Engineering

### Optimization Strategies Implemented

| Challenge | Solution | Impact |
|-----------|----------|--------|
| Slow materialized view (`mv_maternal_health`) | Bypassed with direct `fact_pregnancy` aggregation | Query time: **timeout → <3s** |
| 26 datasets competing for warehouse resources | Staggered refresh with priority-based scheduling | Eliminated "Query cancelled" errors |
| 99.6% dominance of HH visits masking clinical data | Pre-filtered clinical visit dataset | Revealed **Immunization 80.8%, ANC 5.0%** split |
| Monthly `date_key` (YYYYMM) misread as daily | Applied `×22 working days` normalization | Corrected avg visits from **191.8 → 8.7/day** |
| UNKNOWN county records (96K visits, 8.8%) | Root-cause analysis → CHW UUID gap identification | **91.5%** traced to known counties |

### Warehouse Configuration

- **Compute:** Serverless SQL Warehouse (auto-scaling)
- **Caching:** Result caching enabled for repeated filter combinations
- **Concurrency:** Dashboard published with embedded credentials (run-as-owner) to leverage single connection pool

---

## 🔍 Data Quality Framework

### Identified Quality Dimensions

| Issue | Records Affected | Root Cause | Resolution |
|-------|-----------------|------------|------------|
| UNKNOWN county mapping | 96,712 home visits (8.8%) | 558 CHWs with UUIDs absent from `dim_chw` master data | Traced 91.5% to Busia (71.6%) and Kisumu (19.9%) via bronze-layer cross-reference |
| Unmapped facilities | 130 facilities | NULL county/sub-county in `dim_facility` | Identified by facility name pattern matching (e.g., "NAMBALE SUB COUNTY HOSPITAL" → Busia) |
| Temporal data asymmetry | — | Different eCHIS modules have different reporting lag | Documented: Home visits (5mo), Immunization (3mo), Pregnancy (2mo) |

### Data Lineage

```
eCHIS Platform → Bronze (raw, PII retained) → Silver (de-identified, standardized) → Gold (aggregated, modeled)
                                                  ↓                                       ↓
                                           UUID pseudonymization                   Row filters applied
                                           Name/address removal                    Views created
                                           Geographic generalization               Metrics calculated
```

---

## 📁 Repository Structure

```
community-health-intelligence-platform/
│
├── 📊 dashboards/
│   └── community_health_intelligence_platform.lvdash.json    # Full dashboard definition
│
├── 📓 notebooks/
│   ├── chw_semantic_model.ipynb          # Data model & ETL pipeline
│   ├── rls_setup.ipynb                   # Row-Level Security configuration
│   └── chw_rls_setup.ipynb               # CHW-specific RLS setup
│
├── 🗃 sql/
│   ├── datasets/                          # Dashboard dataset queries (24 files)
│   │   ├── _all_datasets.sql             # Combined reference
│   │   ├── executive_kpis.sql            # Executive Command Center queries
│   │   ├── maternal_funnel.sql           # Maternal cascade analysis
│   │   ├── chw_field_ops.sql             # CHW operations queries
│   │   └── ...                           # 21 additional dataset queries
│   ├── schema/
│   │   └── gold_layer_ddl.sql            # Complete gold layer DDL (11 tables, 8 views)
│   ├── security/
│   │   └── rls_setup.sql                 # RLS functions & row filter application
│   ├── rls_functions.sql                 # Standalone RLS function definitions
│   └── sample_queries.sql                # Analytical query examples
│
├── 📖 docs/
│   ├── data_dictionary.md                # Column-level documentation
│   └── rls_access_matrix.md              # Security role assignments
│
├── 🖼 images/                             # Dashboard screenshots
│   ├── 01_executive_command_center.png
│   ├── 02_maternal_continuum_of_care.png
│   └── 03_chw_field_operations.png
│
├── README.md
├── LICENSE
└── .gitignore
```

> **⚠️ No data files are stored in this repository.** All SQL files contain only query definitions and schema DDL. Source data requires authorized access to Kenya's eCHIS platform.

---

## 🚀 Getting Started

### Prerequisites

| Requirement | Detail |
|------------|--------|
| **eCHIS Access** | Authorized credentials from County Health Management Team |
| **Databricks Workspace** | AWS with Unity Catalog enabled |
| **SQL Warehouse** | Serverless recommended for auto-scaling |
| **Catalog** | `community_health_intelligence` with `bronze`, `silver`, `gold` schemas |
| **Permissions** | `CREATE FUNCTION`, `ALTER TABLE` on gold schema for RLS setup |

### Deployment Steps

```bash
# 1. Clone repository into Databricks Git folder
#    Workspace → Create → Git folder → paste repo URL

# 2. Obtain eCHIS data extracts (requires authorized credentials)
#    Contact your County Health Management Team for data access

# 3. Run the data model notebook to create gold-layer tables
#    Open notebooks/chw_semantic_model.ipynb → Run All

# 4. Configure Row-Level Security
#    Open notebooks/rls_setup.ipynb → Run All
#    This creates: RLS functions, user_access_control table, row filters

# 5. Import the dashboard
#    Workspace → Import → Upload dashboards/*.lvdash.json

# 6. Publish with embedded credentials
#    Dashboard → Publish → Enable "Run as owner" → Select warehouse
```

### Cascading Global Filters

The dashboard implements **4 cascading filters** that propagate across all pages:

| Filter | Binds To | Behavior |
|--------|----------|----------|
| **County** | 12 datasets | Primary geographic filter |
| **Sub-County** | 10 datasets | Cascades from county selection |
| **Reporting Month** | 3 datasets | Temporal filter on `reportedm` |
| **Coverage Status** | 2 datasets | On Track / At Risk / Critical |

---

## 🔧 Technical Specifications

| Component | Technology |
|-----------|-----------|
| **Data Source** | Kenya National eCHIS (echis.health.go.ke) |
| **Cloud Platform** | AWS |
| **Analytics Platform** | Databricks Lakehouse |
| **Storage Format** | Delta Lake |
| **Data Governance** | Unity Catalog |
| **Compute** | Serverless SQL Warehouse |
| **Dashboard** | Databricks AI/BI (Lakeview) |
| **Security** | Row-Level Security via SQL UDFs |
| **Privacy** | De-identification at Silver layer; aggregate-only Gold layer |
| **Version Control** | Git (GitHub) |
| **Data Architecture** | Medallion (Bronze → Silver → Gold) |
| **Schema Design** | Star Schema (Kimball methodology) |

### Data Coverage

| Domain | eCHIS Module | Time Range | Grain | Volume |
|--------|-------------|-----------|-------|--------|
| Home Visits | Household Register | Dec 2024 – Apr 2025 | CHW × Month | 3.2M visits |
| Immunization | Child Tracker | Jan 2025 – Mar 2025 | CHW × Month × CU | 5,874 fully immunized |
| Pregnancy/ANC | ANC Register | Jan 2025 – Feb 2025 | Pregnancy record | 25.7% ANC4+ rate |
| Family Planning | FP Register | Jan 2025 – Feb 2025 | Monthly summary | 3.6% mCPR |
| Supervision | Supervision Checklist | Jan 2025 – Feb 2025 | CHW assessment | 4,671 active CHWs |

---

<div align="center">

### Built with

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat-square&logo=delta&logoColor=white)](https://delta.io)

**[Erick Kiprotich Yegon, PhD](https://github.com/erickyegon)** · AI & Data Science Consultant  
17+ years · Global Health · Databricks · Causal Inference · EB-1A  
[LinkedIn](https://linkedin.com/in/erickyegon) · [ORCID](https://orcid.org/0000-0002-7055-4848) · keyegon@gmail.com

*Transforming Kenya's community health data into actionable intelligence — with privacy by design*

</div>
