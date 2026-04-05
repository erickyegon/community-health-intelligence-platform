-- ============================================================================
-- Dataset: ds_dropout_analysis
-- Dashboard Dataset ID: b9cdce7a
-- ============================================================================

-- Dataset: ds_dropout_analysis
SELECT
  dropout_stage,
  maternal_age_group,
  muac_worst                                             AS muac_risk,
  COUNT(*)                                               AS women_count,
  ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1)           AS pct_of_total,
  SUM(had_danger_signs)                                  AS with_danger_signs,
  ROUND(AVG(total_anc_visits),1)                         AS avg_anc_visits,
  ROUND(AVG(age_years),1)                                AS avg_age,
  SUM(pnc_within_48hrs)                                  AS pnc_within_48hrs,
  SUM(facility_delivery)                                 AS facility_deliveries,
  SUM(is_maternal_death)                                 AS maternal_deaths
FROM community_health_intelligence.gold.fact_pregnancy_journey
GROUP BY ALL
ORDER BY women_count DESC
