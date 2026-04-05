-- ============================================================================
-- Dataset: ds_visit_trend
-- Dashboard Dataset ID: daily_visit_trend
-- ============================================================================

SELECT
  hv.date_key,
  CONCAT(
    CASE CAST(hv.date_key % 100 AS INT)
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Feb'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Apr'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Aug'
      WHEN 9 THEN 'Sep'
      WHEN 10 THEN 'Oct'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dec'
    END,
    ' ',
    CAST(CAST(hv.date_key / 100 AS INT) AS STRING)
  ) AS month_label,
  c.county_name,
  c.sub_county_name,
  SUM(hv.visit_count) AS total_visits,
  COUNT(DISTINCT hv.chw_uuid) AS active_chws,
  COUNT(DISTINCT hv.family_id) AS households_reached
FROM
  community_health_intelligence.gold.fact_home_visit hv
    JOIN community_health_intelligence.gold.dim_chw c
      ON hv.chw_uuid = c.chw_uuid
WHERE
  c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
GROUP BY
  hv.date_key,
  c.county_name,
  c.sub_county_name
ORDER BY
  hv.date_key
