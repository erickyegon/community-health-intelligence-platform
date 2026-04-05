-- ============================================================================
-- Dataset: ds_subcounty_comparison
-- Dashboard Dataset ID: 6825e9ca
-- ============================================================================

-- Dataset: ds_subcounty_comparison (fixed population join)
SELECT
  c.sub_county_name,
  c.county_name,
  COUNT(DISTINCT c.chw_uuid)                                      AS chw_count,
  COALESCE(SUM(iz.iz_visits), 0)                                  AS iz_visits,
  COALESCE(SUM(iz.fully_immunized), 0)                            AS fully_immunized,
  ROUND(100.0 * COALESCE(SUM(iz.fully_immunized), 0)
    / NULLIF(COALESCE(SUM(iz.iz_visits), 0), 0), 1)               AS iz_coverage_pct,
  COALESCE(SUM(pop.chw_u5), 0)                                    AS u5_pop,
  COALESCE(SUM(pop.chw_wra), 0)                                   AS wra_pop,
  COALESCE(SUM(pop.chw_u5), 0) - COALESCE(SUM(iz.iz_visits), 0)  AS iz_gap,
  COALESCE(SUM(fp.on_fp), 0)                                      AS fp_clients_on_fp,
  ROUND(100.0 * COALESCE(SUM(fp.on_fp), 0)
    / NULLIF(COALESCE(SUM(pop.chw_wra), 0), 0), 1)                AS mcpr_pct
FROM community_health_intelligence.gold.dim_chw c
LEFT JOIN (
  SELECT chw_uuid,
    COUNT(*)             AS iz_visits,
    SUM(is_fully_immunized) AS fully_immunized
  FROM community_health_intelligence.gold.fact_immunization
  GROUP BY chw_uuid
) iz ON c.chw_uuid = iz.chw_uuid
LEFT JOIN (
  SELECT chw_uuid,
    SUM(u5_pop)  AS chw_u5,
    SUM(wra_pop) AS chw_wra
  FROM community_health_intelligence.gold.fact_population
  GROUP BY chw_uuid
) pop ON c.chw_uuid = pop.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(is_on_fp) AS on_fp
  FROM community_health_intelligence.gold.fact_family_planning
  GROUP BY chw_uuid
) fp ON c.chw_uuid = fp.chw_uuid
WHERE c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
GROUP BY c.sub_county_name, c.county_name
ORDER BY iz_coverage_pct DESC
