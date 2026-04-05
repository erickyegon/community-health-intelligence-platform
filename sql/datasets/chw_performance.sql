-- ============================================================================
-- Dataset: ds_chw_performance
-- Dashboard Dataset ID: edbdd54b
-- ============================================================================

SELECT
  c.chw_name,
  c.community_unit,
  c.sub_county_name,
  c.county_name,
  s.overall_score_pct,
  s.immunization_score_pct,
  s.pregnancy_score_pct,
  s.nutrition_score_pct,
  s.malaria_score_pct,
  s.fp_score_pct,
  s.newborn_score_pct,
  s.wash_score_pct,
  s.has_all_tools,
  s.has_ppe,
  s.visit_count,
  s.reportedm,
  COALESCE(hv.total_home_visits, 0)                                  AS total_home_visits,
  CASE
    WHEN s.overall_score_pct >= 80 THEN 'High performer'
    WHEN s.overall_score_pct >= 60 THEN 'Meeting expectations'
    WHEN s.overall_score_pct >= 40 THEN 'Below average'
    ELSE 'Needs support' END                                         AS performance_tier,
  CASE WHEN s.has_all_tools = 1 AND s.has_ppe = 1 THEN 'Fully equipped'
       WHEN s.has_all_tools = 1 OR s.has_ppe = 1  THEN 'Partially equipped'
       ELSE 'Not equipped' END                                       AS equipment_status
FROM community_health_intelligence.gold.fact_supervision s
JOIN community_health_intelligence.gold.dim_chw c
  ON s.chw_uuid = c.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS total_home_visits
  FROM community_health_intelligence.gold.fact_home_visit
  GROUP BY chw_uuid
) hv ON s.chw_uuid = hv.chw_uuid
WHERE s.overall_score_pct IS NOT NULL
ORDER BY s.overall_score_pct DESC
