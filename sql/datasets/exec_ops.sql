-- ============================================================================
-- Dataset: ds_exec_ops
-- Dashboard Dataset ID: exec_ops
-- ============================================================================

SELECT
  c.county_name,
  c.sub_county_name,
  c.community_unit,
  c.chw_name,
  c.chw_uuid,
  COALESCE(fp.on_fp, 0) AS on_fp,
  COALESCE(pop.wra_pop, 0) AS wra_pop,
  COALESCE(hv.visit_count, 0) AS home_visits,
  COALESCE(iz.iz_visits, 0) AS iz_visits,
  COALESCE(iz.fully_immunized, 0) AS fully_immunized
FROM
  community_health_intelligence.gold.dim_chw c
    LEFT JOIN (
      SELECT
        chw_uuid,
        SUM(is_on_fp) AS on_fp
      FROM
        community_health_intelligence.gold.fact_family_planning
      GROUP BY
        chw_uuid
    ) fp
      ON c.chw_uuid = fp.chw_uuid
    LEFT JOIN (
      SELECT
        chw_uuid,
        SUM(wra_pop) AS wra_pop
      FROM
        community_health_intelligence.gold.fact_population
      GROUP BY
        chw_uuid
    ) pop
      ON c.chw_uuid = pop.chw_uuid
    LEFT JOIN (
      SELECT
        chw_uuid,
        SUM(visit_count) AS visit_count
      FROM
        community_health_intelligence.gold.fact_home_visit
      GROUP BY
        chw_uuid
    ) hv
      ON c.chw_uuid = hv.chw_uuid
    LEFT JOIN (
      SELECT
        chw_uuid,
        COUNT(*) AS iz_visits,
        SUM(is_fully_immunized) AS fully_immunized
      FROM
        community_health_intelligence.gold.fact_immunization
      GROUP BY
        chw_uuid
    ) iz
      ON c.chw_uuid = iz.chw_uuid
WHERE
  c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
