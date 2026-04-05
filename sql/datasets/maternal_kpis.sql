-- ============================================================================
-- Dataset: ds_maternal_kpis
-- Dashboard Dataset ID: 6693cfe0
-- ============================================================================

-- Dataset: ds_maternal_kpis
SELECT
  COUNT(*)                                                              AS total_episodes,
  SUM(stage_5_anc4)                                                     AS anc4_complete,
  ROUND(100.0*SUM(stage_5_anc4)/NULLIF(COUNT(*),0),1)                  AS anc4_completion_pct,
  SUM(stage_7_pnc)                                                      AS pnc_reached,
  ROUND(100.0*SUM(stage_7_pnc)/NULLIF(COUNT(*),0),1)                   AS pnc_coverage_pct,
  SUM(pnc_within_48hrs)                                                 AS pnc_48hrs,
  ROUND(100.0*SUM(pnc_within_48hrs)/NULLIF(SUM(stage_7_pnc),0),1)      AS pnc_48hr_rate,
  SUM(facility_delivery)                                                AS facility_deliveries,
  ROUND(100.0*SUM(facility_delivery)/NULLIF(SUM(stage_6_delivery),0),1) AS facility_delivery_rate,
  SUM(had_danger_signs)                                                 AS danger_sign_cases,
  ROUND(100.0*SUM(had_danger_signs)/NULLIF(COUNT(*),0),1)               AS danger_sign_rate,
  SUM(is_maternal_death)                                                AS maternal_deaths,
  SUM(ever_on_iron_folate)                                              AS on_iron_folate,
  ROUND(100.0*SUM(ever_on_iron_folate)/NULLIF(COUNT(*),0),1)            AS iron_folate_coverage,
  -- Biggest dropout stage
  COUNT(*) - SUM(stage_2_anc1)                                         AS dropout_before_anc1,
  SUM(stage_3_anc2) - SUM(stage_4_anc3)                               AS dropout_anc2_anc3,
  SUM(stage_4_anc3) - SUM(stage_5_anc4)                               AS dropout_anc3_anc4
FROM community_health_intelligence.gold.fact_pregnancy_journey
