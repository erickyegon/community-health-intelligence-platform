-- =============================================================================
-- Row-Level Security Functions
-- Community Health Intelligence Platform
-- =============================================================================

-- User Access Control Table
CREATE TABLE IF NOT EXISTS community_health_intelligence.gold.user_access_control (
  user_email STRING NOT NULL,
  access_level STRING NOT NULL,  -- ADMIN, COUNTY, SUBCOUNTY
  county_name STRING,
  sub_county_name STRING,
  CONSTRAINT valid_access_level CHECK (access_level IN ('ADMIN', 'COUNTY', 'SUBCOUNTY'))
);

-- County-level access filter function
CREATE OR REPLACE FUNCTION community_health_intelligence.gold.county_access_filter(county STRING)
RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM community_health_intelligence.gold.user_access_control
  WHERE user_email = CURRENT_USER()
    AND (
      access_level = 'ADMIN'
      OR (access_level IN ('COUNTY', 'SUBCOUNTY') AND county_name = county)
    )
);

-- Sub-county-level access filter function (cascading)
CREATE OR REPLACE FUNCTION community_health_intelligence.gold.subcounty_access_filter(county STRING, sub_county STRING)
RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM community_health_intelligence.gold.user_access_control
  WHERE user_email = CURRENT_USER()
    AND (
      access_level = 'ADMIN'
      OR (access_level = 'COUNTY' AND county_name = county)
      OR (access_level = 'SUBCOUNTY' AND county_name = county AND sub_county_name = sub_county)
    )
);

-- Apply row filters to dimension tables
ALTER TABLE community_health_intelligence.gold.dim_chw
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

ALTER TABLE community_health_intelligence.gold.dim_facility
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

ALTER TABLE community_health_intelligence.gold.dim_geography
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

-- Apply row filters to fact tables
ALTER TABLE community_health_intelligence.gold.fact_home_visit
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_immunization
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pregnancy
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pregnancy_journey
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_family_planning
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pnc
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_population
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_supervision
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);
