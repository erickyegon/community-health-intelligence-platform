-- ============================================================================
-- Dataset: ds_coverage_kpis
-- Dashboard Dataset ID: 5a00995f
-- ============================================================================

SELECT
  SUM(u5_pop)                                                        AS total_u5_pop,
  SUM(wra_pop)                                                       AS total_wra_pop,
  SUM(iz_visits)                                                     AS total_iz_visits,
  SUM(iz_unserved)                                                   AS total_iz_unserved,
  ROUND(100.0*SUM(fully_immunized)/NULLIF(SUM(u5_pop),0),1)         AS overall_iz_coverage_pct,
  SUM(fp_on_fp)                                                      AS total_on_fp,
  SUM(fp_unserved_wra)                                               AS total_fp_unserved,
  ROUND(100.0*SUM(fp_on_fp)/NULLIF(SUM(wra_pop),0),1)               AS overall_mcpr_pct,
  SUM(home_visits)                                                   AS total_home_visits,
  COUNT(DISTINCT chw_uuid)                                           AS chw_areas_covered,
  COUNT(DISTINCT sub_county_name)                                    AS sub_counties_covered
FROM (
  SELECT
    c.chw_uuid, c.sub_county_name,
    COALESCE(pop.u5_pop,0) AS u5_pop, COALESCE(pop.wra_pop,0) AS wra_pop,
    COALESCE(iz.iz_visits,0) AS iz_visits,
    COALESCE(iz.fully_immunized,0) AS fully_immunized,
    COALESCE(pop.u5_pop,0)-COALESCE(iz.iz_visits,0) AS iz_unserved,
    COALESCE(fp.on_fp,0) AS fp_on_fp,
    COALESCE(pop.wra_pop,0)-COALESCE(fp.on_fp,0) AS fp_unserved_wra,
    COALESCE(hv.home_visits,0) AS home_visits
  FROM community_health_intelligence.gold.dim_chw c
  LEFT JOIN (SELECT chw_uuid, SUM(u5_pop) AS u5_pop, SUM(wra_pop) AS wra_pop,
    SUM(u2_pop) AS u2_pop FROM community_health_intelligence.gold.fact_population
    GROUP BY chw_uuid) pop ON c.chw_uuid = pop.chw_uuid
  LEFT JOIN (SELECT chw_uuid, COUNT(*) AS iz_visits,
    SUM(is_fully_immunized) AS fully_immunized
    FROM community_health_intelligence.gold.fact_immunization
    GROUP BY chw_uuid) iz ON c.chw_uuid = iz.chw_uuid
  LEFT JOIN (SELECT chw_uuid, SUM(is_on_fp) AS on_fp
    FROM community_health_intelligence.gold.fact_family_planning
    GROUP BY chw_uuid) fp ON c.chw_uuid = fp.chw_uuid
  LEFT JOIN (SELECT chw_uuid, SUM(visit_count) AS home_visits
    FROM community_health_intelligence.gold.fact_home_visit
    GROUP BY chw_uuid) hv ON c.chw_uuid = hv.chw_uuid
)
