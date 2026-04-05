-- ============================================================================
-- Dataset: ds_chw_field_ops
-- Dashboard Dataset ID: chw_field_ops
-- ============================================================================

SELECT
  c.county_name,
  c.sub_county_name,
  c.community_unit,
  c.chw_uuid,
  COALESCE(hv.total_visits, 0) AS home_visits,
  COALESCE(hv.unique_households, 0) AS unique_households,
  COALESCE(hv.active_days, 0) AS active_days,
  COALESCE(iz.iz_visits, 0) AS iz_visits,
  COALESCE(iz.defaulter_traces, 0) AS iz_defaulter_traces,
  COALESCE(iz.fully_immunized, 0) AS fully_immunized
FROM
  community_health_intelligence.gold.dim_chw c
    LEFT JOIN (
      SELECT
        chw_uuid,
        SUM(visit_count) AS total_visits,
        COUNT(DISTINCT family_id) AS unique_households,
        COUNT(DISTINCT date_key) AS active_days
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
        SUM(is_defaulter) AS defaulter_traces,
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
