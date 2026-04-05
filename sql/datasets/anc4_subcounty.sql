-- ============================================================================
-- Dataset: ds_anc4_subcounty
-- Dashboard Dataset ID: anc4_subcounty
-- ============================================================================

SELECT
  sub_county_clean AS sub_county_name,
  county_clean AS county_name,
  COUNT(*) AS total_pregnancies,
  SUM(anc_complete) AS anc4_complete,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_rate
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  sub_county_clean IS NOT NULL
  AND sub_county_clean != ''
GROUP BY
  sub_county_clean,
  county_clean
ORDER BY
  anc4_rate DESC
