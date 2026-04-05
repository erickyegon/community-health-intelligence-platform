-- ============================================================================
-- Dataset: ds_maternal_page_kpis
-- Dashboard Dataset ID: maternal_page_kpis
-- ============================================================================

SELECT
  j.anc1_rate,
  j.anc2_rate,
  j.anc4_rate,
  j.skilled_delivery_rate,
  j.pnc1_rate,
  j.pnc_48hr_rate,
  j.total_registered,
  j.facility_delivery_pct,
  p.anc_defaulter_rate,
  ROUND(100.0 * fp.on_fp / NULLIF(pop.wra_pop, 0), 1) AS mcpr_pct
FROM
  (
    SELECT
      COUNT(*) AS total_registered,
      ROUND(100.0 * SUM(stage_2_anc1) / NULLIF(COUNT(*), 0), 1) AS anc1_rate,
      ROUND(100.0 * SUM(stage_3_anc2) / NULLIF(COUNT(*), 0), 1) AS anc2_rate,
      ROUND(100.0 * SUM(stage_5_anc4) / NULLIF(COUNT(*), 0), 1) AS anc4_rate,
      ROUND(
        100.0 * SUM(facility_delivery) / NULLIF(SUM(stage_6_delivery), 0),
        1
      ) AS skilled_delivery_rate,
      ROUND(100.0 * SUM(stage_7_pnc) / NULLIF(COUNT(*), 0), 1) AS pnc1_rate,
      ROUND(100.0 * SUM(pnc_within_48hrs) / NULLIF(SUM(stage_7_pnc), 0), 1) AS pnc_48hr_rate,
      ROUND(100.0 * SUM(facility_delivery) / NULLIF(COUNT(*), 0), 1) AS facility_delivery_pct
    FROM
      community_health_intelligence.gold.fact_pregnancy_journey
  ) j,
  (
    SELECT
      ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_rate
    FROM
      community_health_intelligence.gold.fact_pregnancy
  ) p,
  (
    SELECT
      SUM(is_on_fp) AS on_fp
    FROM
      community_health_intelligence.gold.fact_family_planning
  ) fp,
  (
    SELECT
      SUM(wra_pop) AS wra_pop
    FROM
      community_health_intelligence.gold.fact_population
  ) pop
