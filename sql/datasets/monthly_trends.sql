-- ============================================================================
-- Dataset: ds_monthly_trends
-- Dashboard Dataset ID: 26dac079
-- ============================================================================

SELECT
  reportedm,
  county_clean,
  sub_county_clean,
  COUNT(*) AS registrations,
  SUM(anc_complete) AS anc4_count,
  SUM(is_anc_defaulter) AS defaulter_count,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_pct,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS defaulter_pct
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  county_clean IS NOT NULL
  AND county_clean != ''
GROUP BY
  reportedm,
  county_clean,
  sub_county_clean
ORDER BY
  reportedm
