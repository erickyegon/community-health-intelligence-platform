-- ============================================================================
-- Dataset: ds_supervision_domain_scores
-- Dashboard Dataset ID: 6254a886
-- ============================================================================

-- Unpivoted domain scores for radar/heatmap
WITH scores AS (
  SELECT
    c.chw_name,
    c.sub_county_name,
    c.county_name,
    s.overall_score_pct,
    s.immunization_score_pct,
    s.pregnancy_score_pct,
    s.nutrition_score_pct,
    s.malaria_score_pct,
    s.fp_score_pct,
    s.newborn_score_pct,
    s.wash_score_pct
  FROM community_health_intelligence.gold.fact_supervision s
  JOIN community_health_intelligence.gold.dim_chw c ON s.chw_uuid = c.chw_uuid
  WHERE s.overall_score_pct IS NOT NULL
)
SELECT domain, ROUND(AVG(score), 1) AS avg_score,
  ROUND(MIN(score), 1) AS min_score,
  ROUND(MAX(score), 1) AS max_score,
  COUNT(*) AS chw_count
FROM (
  SELECT 'Immunization'   AS domain, immunization_score_pct AS score FROM scores
  UNION ALL
  SELECT 'Pregnancy',              pregnancy_score_pct     FROM scores
  UNION ALL
  SELECT 'Nutrition',              nutrition_score_pct     FROM scores
  UNION ALL
  SELECT 'Malaria',                malaria_score_pct       FROM scores
  UNION ALL
  SELECT 'Family Planning',        fp_score_pct            FROM scores
  UNION ALL
  SELECT 'Newborn Care',           newborn_score_pct       FROM scores
  UNION ALL
  SELECT 'WASH',                   wash_score_pct          FROM scores
)
WHERE score IS NOT NULL
GROUP BY domain
ORDER BY avg_score DESC
