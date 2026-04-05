-- ============================================================================
-- Dataset: ds_coverage_gaps
-- Dashboard Dataset ID: eb770221
-- ============================================================================

SELECT
  c.county_name,
  c.sub_county_name,
  c.community_unit,
  c.chw_name,
  c.chw_uuid,
  COALESCE(pop.u2_pop, 0)                                           AS u2_pop,
  COALESCE(pop.u5_pop, 0)                                           AS u5_pop,
  COALESCE(pop.wra_pop, 0)                                          AS wra_pop,
  COALESCE(iz.iz_visits, 0)                                         AS iz_visits,
  COALESCE(iz.fully_immunized, 0)                                   AS fully_immunized,
  COALESCE(pop.u5_pop, 0) - COALESCE(iz.iz_visits, 0)              AS iz_unserved,
  ROUND(100.0*COALESCE(iz.fully_immunized,0)
    /NULLIF(COALESCE(pop.u5_pop,0),0),1)                            AS iz_coverage_pct,
  COALESCE(fp.on_fp, 0)                                             AS fp_on_fp,
  COALESCE(pop.wra_pop,0) - COALESCE(fp.on_fp,0)                   AS fp_unserved_wra,
  ROUND(100.0*COALESCE(fp.on_fp,0)
    /NULLIF(COALESCE(pop.wra_pop,0),0),1)                           AS mcpr_pct,
  COALESCE(hv.home_visits, 0)                                       AS home_visits,
  ROUND(100.0*COALESCE(hv.home_visits,0)
    /NULLIF(COALESCE(pop.u5_pop,0),0),1)                            AS home_visit_coverage_pct
FROM community_health_intelligence.gold.dim_chw c
LEFT JOIN (
  SELECT chw_uuid,
    SUM(u5_pop) AS u5_pop, SUM(u2_pop) AS u2_pop, SUM(wra_pop) AS wra_pop
  FROM community_health_intelligence.gold.fact_population GROUP BY chw_uuid
) pop ON c.chw_uuid = pop.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, COUNT(*) AS iz_visits, SUM(is_fully_immunized) AS fully_immunized
  FROM community_health_intelligence.gold.fact_immunization GROUP BY chw_uuid
) iz ON c.chw_uuid = iz.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(is_on_fp) AS on_fp
  FROM community_health_intelligence.gold.fact_family_planning GROUP BY chw_uuid
) fp ON c.chw_uuid = fp.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS home_visits
  FROM community_health_intelligence.gold.fact_home_visit GROUP BY chw_uuid
) hv ON c.chw_uuid = hv.chw_uuid
WHERE c.sub_county_name IS NOT NULL AND c.sub_county_name != ''
ORDER BY iz_coverage_pct ASC
