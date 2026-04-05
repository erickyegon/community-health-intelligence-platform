-- ============================================================================
-- Dataset: ds_chw_kpis
-- Dashboard Dataset ID: 8d3a8d33
-- ============================================================================

SELECT
  COUNT(DISTINCT s.chw_uuid)                                        AS total_supervised,
  ROUND(AVG(s.overall_score_pct), 1)                                AS avg_overall_score,
  SUM(CASE WHEN s.overall_score_pct >= 80 THEN 1 ELSE 0 END)        AS high_performers,
  SUM(CASE WHEN s.overall_score_pct >= 60
        AND s.overall_score_pct < 80 THEN 1 ELSE 0 END)             AS meeting_expectations,
  SUM(CASE WHEN s.overall_score_pct < 40 THEN 1 ELSE 0 END)         AS needs_support,
  SUM(s.has_all_tools)                                              AS fully_equipped,
  SUM(s.has_ppe)                                                    AS has_ppe,
  ROUND(100.0*SUM(s.has_all_tools)/NULLIF(COUNT(*),0),1)            AS tools_readiness_pct,
  ROUND(100.0*SUM(s.has_ppe)/NULLIF(COUNT(*),0),1)                  AS ppe_readiness_pct,
  SUM(hv.total_home_visits)                                         AS total_home_visits,
  ROUND(AVG(hv.total_home_visits),0)                                AS avg_visits_per_chw
FROM community_health_intelligence.gold.fact_supervision s
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS total_home_visits
  FROM community_health_intelligence.gold.fact_home_visit
  GROUP BY chw_uuid
) hv ON s.chw_uuid = hv.chw_uuid
WHERE s.overall_score_pct IS NOT NULL
