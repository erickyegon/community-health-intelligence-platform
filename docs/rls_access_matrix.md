# Row-Level Security Access Matrix

## Community Health Intelligence Platform

### Access Levels

| Level | Description | Filter Behavior |
|-------|-------------|-----------------|
| **ADMIN** | Full access to all data | No filtering applied |
| **COUNTY** | Access to all sub-counties within assigned county | Filters by county_name |
| **SUBCOUNTY** | Access to single sub-county only | Filters by county_name AND sub_county_name |

### User Assignments

| User Email | Level | County | Sub-County |
|------------|-------|--------|------------|
| keyegonaws@gmail.com | ADMIN | ALL | ALL |
| keyegon@gmail.com | COUNTY | BUSIA | ALL |
| erickkiprotichyegon61@gmail.com | SUBCOUNTY | BUSIA | TESO NORTH |
| admin@example.com | ADMIN | ALL | ALL |
| busia.manager@example.com | COUNTY | BUSIA | ALL |
| kisumu.manager@example.com | COUNTY | KISUMU | ALL |
| teso.north.officer@example.com | SUBCOUNTY | BUSIA | TESO NORTH |
| matayos.officer@example.com | SUBCOUNTY | BUSIA | MATAYOS |
| kisumu.central.officer@example.com | SUBCOUNTY | KISUMU | KISUMU CENTRAL |

### Tables with Row Filters (11)

#### Dimension Tables (3)
| Table | Filter Function | Column Mapping |
|-------|----------------|----------------|
| `dim_chw` | `subcounty_access_filter` | county_name, sub_county_name |
| `dim_facility` | `subcounty_access_filter` | county_name, sub_county_name |
| `dim_geography` | `subcounty_access_filter` | county_name, sub_county_name |

#### Fact Tables (8)
| Table | Filter Function | Column Mapping |
|-------|----------------|----------------|
| `fact_family_planning` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_home_visit` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_immunization` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_pnc` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_population` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_pregnancy` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_pregnancy_journey` | `subcounty_access_filter` | county_clean, sub_county_clean |
| `fact_supervision` | `subcounty_access_filter` | county_clean, sub_county_clean |

### Views with Inherited RLS (8)
These views query filtered tables and automatically inherit row-level security:
- `mv_family_planning`
- `mv_immunization`
- `mv_maternal_health`
- `mv_supervision`
- `vw_chw_performance`
- `vw_coverage_gaps`
- `vw_executive_summary`
- `vw_maternal_funnel`

### Testing RLS
```sql
-- Verify current user access
SELECT * FROM community_health_intelligence.gold.user_access_control
WHERE user_email = CURRENT_USER();

-- Test county filter
SELECT DISTINCT county_clean
FROM community_health_intelligence.gold.fact_home_visit;

-- Test sub-county filter
SELECT DISTINCT county_clean, sub_county_clean
FROM community_health_intelligence.gold.fact_home_visit;
```
