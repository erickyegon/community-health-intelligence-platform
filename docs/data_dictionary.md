# Data Dictionary

## Community Health Intelligence Platform

### Catalog: `community_health_intelligence`

---

## Gold Layer Tables

### Dimension Tables

#### `dim_chw` — Community Health Worker Master
| Column | Type | Description |
|--------|------|-------------|
| chw_area_uuid | STRING | Primary key — unique CHW identifier |
| chw_name | STRING | Worker name |
| county_name | STRING | County assignment (RLS filtered) |
| sub_county_name | STRING | Sub-county assignment (RLS filtered) |
| community_unit | STRING | Community unit assignment |
| facility_name | STRING | Linked health facility |
| status | STRING | Active/Inactive status |

#### `dim_facility` — Health Facility Master
| Column | Type | Description |
|--------|------|-------------|
| facility_id | STRING | Primary key |
| facility_name | STRING | Facility name |
| county_name | STRING | County (RLS filtered) |
| sub_county_name | STRING | Sub-county (RLS filtered) |
| facility_type | STRING | Type (Hospital, Health Center, etc.) |

#### `dim_geography` — Geographic Hierarchy
| Column | Type | Description |
|--------|------|-------------|
| county_name | STRING | County name (RLS filtered) |
| sub_county_name | STRING | Sub-county name (RLS filtered) |
| ward_name | STRING | Ward name |

---

### Fact Tables

#### `fact_home_visit` — CHW Household Visit Records
| Column | Type | Description |
|--------|------|-------------|
| chw_area_uuid | STRING | FK to dim_chw |
| date_key | INT | YYYYMM format (monthly grain) |
| county_clean | STRING | Standardized county (UPPER CASE) |
| sub_county_clean | STRING | Standardized sub-county |
| home_visits | BIGINT | Count of household visits |
| active_days | INT | COUNT(DISTINCT date_key) — months active |
| unique_beneficiaries | BIGINT | Distinct beneficiaries visited |
| visit_type | STRING | HH Visit, Immunization, ANC, Defaulter |

#### `fact_immunization` — Immunization Records
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |
| community_unit | STRING | Community unit |
| penta3 | INT | Penta3 doses administered |
| fully_immunized | INT | Fully immunized children |
| target_pop | INT | Target population for coverage calc |
| reportedm | DATE | Reporting month |

#### `fact_pregnancy` — Pregnancy Registration & ANC
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |
| reportedm | DATE | Reporting month |
| anc_complete | INT | 1 if completed ANC4+ visits, 0 otherwise |
| is_anc_defaulter | INT | 1 if ANC defaulter, 0 otherwise |
| anc1_count | INT | ANC1 visit completed |
| anc2_count | INT | ANC2 visit completed |

#### `fact_family_planning` — Family Planning Services
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |
| on_fp | INT | Women currently on family planning |
| wra_pop | INT | Women of reproductive age population |

#### `fact_pnc` — Postnatal Care
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |
| pnc_visits | INT | PNC visits completed |

#### `fact_pregnancy_journey` — End-to-End Pregnancy Tracking
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |
| anc1_done through anc4_done | INT | ANC visit completion flags |
| skilled_delivery | INT | Delivery at facility flag |
| pnc1_done | INT | PNC within 48 hours flag |

#### `fact_supervision` — CHW Supervision Records
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |

#### `fact_population` — Population Denominators
| Column | Type | Description |
|--------|------|-------------|
| county_clean | STRING | County |
| sub_county_clean | STRING | Sub-county |

---

### Key Calculated Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| Penta3 Coverage % | `100 * SUM(penta3) / SUM(target_pop)` | Immunization coverage rate |
| ANC4+ Rate % | `100 * SUM(anc_complete) / COUNT(*)` | Completed 4+ ANC visits |
| Defaulter Rate % | `100 * SUM(is_anc_defaulter) / COUNT(*)` | ANC defaulter rate |
| mCPR % | `100 * SUM(on_fp) / SUM(wra_pop)` | Modern contraceptive prevalence |
| Skilled Delivery % | `100 * SUM(skilled_delivery) / COUNT(*)` | Facility-based deliveries |
| PNC1 % | `100 * SUM(pnc1_done) / COUNT(*)` | Postnatal care within 48hrs |
| Avg Daily Visits | `SUM(home_visits) / (SUM(active_days) * 22)` | CHW daily visit average |

---

### Coverage Status Thresholds
| Status | Criteria | Color |
|--------|----------|-------|
| On Track | >= 80% | Green (#10B981) |
| At Risk | 50-79% | Amber (#F59E0B) |
| Critical | < 50% | Red (#EF4444) |
