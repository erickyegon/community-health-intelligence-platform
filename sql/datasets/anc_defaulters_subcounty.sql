-- ============================================================================
-- Dataset: ds_anc_defaulters_subcounty
-- Dashboard Dataset ID: anc_defaulters_subcounty
-- ============================================================================

SELECT
  sub_county_clean AS sub_county_name,
  county_clean AS county_name,
  COUNT(*) AS total_pregnancies,
  SUM(is_anc_defaulter) AS anc_defaulters,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_rate
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  sub_county_clean IS NOT NULL
  AND sub_county_clean != ''
GROUP BY
  sub_county_clean,
  county_clean
ORDER BY
  anc_defaulter_rate DESC
