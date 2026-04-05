-- ============================================================================
-- Dataset: ds_exec_iz
-- Dashboard Dataset ID: exec_iz
-- ============================================================================

SELECT
  county_clean AS county_name,
  sub_county_clean AS sub_county_name,
  community_unit_clean AS community_unit,
  reportedm,
  chw_uuid,
  immunization_visits,
  fully_immunized_count,
  immunization_coverage_pct,
  defaulter_count,
  defaulter_rate_pct,
  penta1_count,
  penta3_count,
  penta_dropout_rate_pct,
  bcg_count,
  measles_9m_count,
  vitamin_a_count,
  referral_count
FROM
  community_health_intelligence.gold.mv_immunization
WHERE
  county_clean IS NOT NULL
  AND county_clean != ''
  AND county_clean != 'UNKNOWN'
