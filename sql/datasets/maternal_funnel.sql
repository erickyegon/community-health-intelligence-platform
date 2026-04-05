-- ============================================================================
-- Dataset: ds_maternal_funnel
-- Dashboard Dataset ID: c6ea1673
-- ============================================================================

-- Dataset: ds_maternal_funnel
SELECT
  county_clean,
  COUNT(*)                                                          AS total_registered,
  SUM(stage_2_anc1)                                                 AS reached_anc1,
  SUM(stage_3_anc2)                                                 AS reached_anc2,
  SUM(stage_4_anc3)                                                 AS reached_anc3,
  SUM(stage_5_anc4)                                                 AS reached_anc4_plus,
  SUM(stage_6_delivery)                                             AS reached_delivery,
  SUM(stage_7_pnc)                                                  AS reached_pnc,
  SUM(stage_8_fp)                                                   AS reached_fp_postpartum,
  -- Conversion rates
  ROUND(100.0*SUM(stage_2_anc1)/NULLIF(COUNT(*),0),1)              AS pct_anc1,
  ROUND(100.0*SUM(stage_3_anc2)/NULLIF(COUNT(*),0),1)              AS pct_anc2,
  ROUND(100.0*SUM(stage_4_anc3)/NULLIF(COUNT(*),0),1)              AS pct_anc3,
  ROUND(100.0*SUM(stage_5_anc4)/NULLIF(COUNT(*),0),1)              AS pct_anc4_complete,
  ROUND(100.0*SUM(stage_6_delivery)/NULLIF(COUNT(*),0),1)          AS pct_delivery,
  ROUND(100.0*SUM(stage_7_pnc)/NULLIF(COUNT(*),0),1)               AS pct_pnc,
  ROUND(100.0*SUM(stage_8_fp)/NULLIF(COUNT(*),0),1)                AS pct_fp_postpartum,
  -- Dropout between stages
  COUNT(*) - SUM(stage_2_anc1)                                     AS dropout_before_anc1,
  SUM(stage_2_anc1) - SUM(stage_3_anc2)                           AS dropout_anc1_anc2,
  SUM(stage_3_anc2) - SUM(stage_4_anc3)                           AS dropout_anc2_anc3,
  SUM(stage_4_anc3) - SUM(stage_5_anc4)                           AS dropout_anc3_anc4,
  SUM(stage_5_anc4) - SUM(stage_6_delivery)                       AS dropout_anc4_delivery,
  SUM(stage_6_delivery) - SUM(stage_7_pnc)                        AS dropout_delivery_pnc,
  -- Outcomes
  SUM(pnc_within_48hrs)                                            AS pnc_within_48hrs,
  SUM(facility_delivery)                                           AS facility_deliveries,
  SUM(had_danger_signs)                                            AS total_danger_signs,
  SUM(is_maternal_death)                                           AS maternal_deaths,
  SUM(ever_on_iron_folate)                                         AS on_iron_folate,
  ROUND(100.0*SUM(pnc_within_48hrs)/NULLIF(SUM(stage_7_pnc),0),1) AS pct_pnc_48hrs,
  ROUND(100.0*SUM(facility_delivery)/NULLIF(SUM(stage_6_delivery),0),1) AS pct_facility_delivery,
  ROUND(100.0*SUM(ever_on_iron_folate)/NULLIF(COUNT(*),0),1)       AS pct_iron_folate
FROM community_health_intelligence.gold.fact_pregnancy_journey
GROUP BY county_clean
