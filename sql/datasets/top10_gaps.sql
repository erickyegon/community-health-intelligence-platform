-- ============================================================================
-- Dataset: ds_top10_gaps
-- Dashboard Dataset ID: 02d71686
-- ============================================================================

SELECT
  c.sub_county_name,
  c.county_name,
  SUM(COALESCE(pop.u5_pop,0))                                       AS u5_pop,
  SUM(COALESCE(iz.iz_visits,0))                                     AS iz_visits,
  SUM(COALESCE(pop.u5_pop,0)) - SUM(COALESCE(iz.iz_visits,0))      AS iz_unserved,
  ROUND(100.0*SUM(COALESCE(iz.fully_immunized,0))
    /NULLIF(SUM(COALESCE(pop.u5_pop,0)),0),1)                       AS iz_coverage_pct,
  SUM(COALESCE(pop.wra_pop,0))                                      AS wra_pop,
  SUM(COALESCE(fp.on_fp,0))                                         AS fp_on_fp,
  SUM(COALESCE(pop.wra_pop,0)) - SUM(COALESCE(fp.on_fp,0))         AS fp_unserved,
  ROUND(100.0*SUM(COALESCE(fp.on_fp,0))
    /NULLIF(SUM(COALESCE(pop.wra_pop,0)),0),1)                      AS mcpr_pct,
  COUNT(DISTINCT c.chw_uuid)                                        AS chw_count
FROM community_health_intelligence.gold.dim_chw c
LEFT JOIN (
  SELECT chw_uuid,
    SUM(u5_pop) AS u5_pop, SUM(wra_pop) AS wra_pop
  FROM community_health_intelligence.gold.fact_population
  GROUP BY chw_uuid
) pop ON c.chw_uuid = pop.chw_uuid
LEFT JOIN (
  SELECT chw_uuid,
    COUNT(*) AS iz_visits,
    SUM(is_fully_immunized) AS fully_immunized
  FROM community_health_intelligence.gold.fact_immunization
  GROUP BY chw_uuid
) iz ON c.chw_uuid = iz.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(is_on_fp) AS on_fp
  FROM community_health_intelligence.gold.fact_family_planning
  GROUP BY chw_uuid
) fp ON c.chw_uuid = fp.chw_uuid
WHERE c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
  AND c.county_name IS NOT NULL
GROUP BY c.sub_county_name, c.county_name
ORDER BY iz_unserved DESC
LIMIT 10
