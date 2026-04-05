-- =============================================================================
-- Sample Dashboard Queries
-- Community Health Intelligence Platform
-- =============================================================================

-- ==========================================
-- EXECUTIVE COMMAND CENTER
-- ==========================================

-- Executive KPIs
SELECT
  ROUND(100.0 * SUM(penta3) / NULLIF(SUM(target_pop), 0), 1) AS penta3_pct,
  SUM(fully_immunized) AS fully_immunized_count,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_pct,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS defaulter_pct,
  ROUND(100.0 * SUM(on_fp) / NULLIF(SUM(wra_pop), 0), 1) AS mcpr_pct,
  COUNT(DISTINCT chw_area_uuid) AS active_chws
FROM community_health_intelligence.gold.vw_executive_summary;

-- Sub-county Immunization Rankings
SELECT
  sub_county_clean AS sub_county_name,
  county_clean AS county_name,
  ROUND(100.0 * SUM(penta3) / NULLIF(SUM(target_pop), 0), 1) AS iz_coverage_pct,
  CASE
    WHEN ROUND(100.0 * SUM(penta3) / NULLIF(SUM(target_pop), 0), 1) >= 80 THEN 'On Track'
    WHEN ROUND(100.0 * SUM(penta3) / NULLIF(SUM(target_pop), 0), 1) >= 50 THEN 'At Risk'
    ELSE 'Critical'
  END AS `Coverage Status`
FROM community_health_intelligence.gold.mv_immunization
WHERE county_clean IS NOT NULL AND county_clean != 'UNKNOWN'
GROUP BY sub_county_clean, county_clean
HAVING iz_coverage_pct IS NOT NULL
ORDER BY iz_coverage_pct DESC;

-- ==========================================
-- MATERNAL CONTINUUM OF CARE
-- ==========================================

-- ANC Monthly Trends
SELECT
  reportedm,
  county_clean,
  sub_county_clean,
  COUNT(*) AS registrations,
  SUM(anc_complete) AS anc4_count,
  SUM(is_anc_defaulter) AS defaulter_count,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_pct,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS defaulter_pct
FROM community_health_intelligence.gold.fact_pregnancy
WHERE county_clean IS NOT NULL AND county_clean != ''
GROUP BY reportedm, county_clean, sub_county_clean
ORDER BY reportedm;

-- ==========================================
-- CHW FIELD OPERATIONS
-- ==========================================

-- CHW Performance Summary
SELECT
  county_clean,
  sub_county_clean,
  COUNT(DISTINCT chw_area_uuid) AS active_chws,
  SUM(home_visits) AS total_visits,
  ROUND(SUM(home_visits) * 1.0 / NULLIF(SUM(active_days) * 22, 0), 1) AS avg_daily_visits,
  SUM(unique_beneficiaries) AS beneficiaries
FROM community_health_intelligence.gold.fact_home_visit
WHERE county_clean IS NOT NULL AND county_clean != 'UNKNOWN'
GROUP BY county_clean, sub_county_clean
ORDER BY county_clean, sub_county_clean;
