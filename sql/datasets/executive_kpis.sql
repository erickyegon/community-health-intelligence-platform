-- ============================================================================
-- Dataset: ds_executive_kpis
-- Dashboard Dataset ID: 6e451adb
-- ============================================================================

WITH iz AS (
  SELECT
    COUNT(*) AS total_iz_visits,
    SUM(is_fully_immunized) AS fully_immunized,
    ROUND(100.0 * SUM(is_fully_immunized) / NULLIF(COUNT(*), 0), 1) AS immunization_coverage_pct,
    SUM(penta1) AS penta1_total,
    SUM(penta3) AS penta3_total,
    ROUND(100.0 * SUM(penta3) / NULLIF(COUNT(*), 0), 1) AS penta3_coverage_pct,
    ROUND(100.0 * (SUM(penta1) - SUM(penta3)) / NULLIF(SUM(penta1), 0), 1) AS penta_dropout_pct,
    SUM(is_defaulter) AS iz_defaulters
  FROM
    community_health_intelligence.gold.fact_immunization
),
preg AS (
  SELECT
    COUNT(*) AS total_pregnancies,
    SUM(anc_complete) AS anc_4plus_count,
    ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc_4plus_pct,
    SUM(is_anc_defaulter) AS anc_defaulters,
    ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_pct,
    SUM(has_danger_signs) AS danger_sign_cases
  FROM
    community_health_intelligence.gold.fact_pregnancy
),
pnc AS (
  SELECT
    COUNT(*) AS total_pnc_visits,
    SUM(pnc_upto_date) AS pnc_upto_date,
    ROUND(100.0 * SUM(pnc_upto_date) / NULLIF(COUNT(*), 0), 1) AS pnc_coverage_pct,
    SUM(is_maternal_death) AS maternal_deaths
  FROM
    community_health_intelligence.gold.fact_pnc
),
fp AS (
  SELECT
    COUNT(DISTINCT patient_id) AS fp_clients,
    SUM(is_on_fp) AS on_fp,
    SUM(refilled_today) AS fp_refills
  FROM
    community_health_intelligence.gold.fact_family_planning
),
pop AS (
  SELECT
    SUM(u2_pop) AS total_u2_pop,
    SUM(u5_pop) AS total_u5_pop,
    SUM(wra_pop) AS total_wra_pop
  FROM
    community_health_intelligence.gold.fact_population
),
chw AS (
  SELECT
    COUNT(DISTINCT chw_uuid) AS active_chws
  FROM
    community_health_intelligence.gold.dim_chw
),
hv AS (
  SELECT
    SUM(visit_count) AS total_home_visits,
    COUNT(DISTINCT family_id) AS households_visited
  FROM
    community_health_intelligence.gold.fact_home_visit
),
sup AS (
  SELECT
    ROUND(AVG(overall_score_pct), 1) AS avg_supervision_score,
    SUM(
      CASE
        WHEN overall_score_pct >= 80 THEN 1
        ELSE 0
      END
    ) AS high_performers,
    SUM(
      CASE
        WHEN overall_score_pct < 40 THEN 1
        ELSE 0
      END
    ) AS needs_support
  FROM
    community_health_intelligence.gold.fact_supervision
  WHERE
    overall_score_pct IS NOT NULL
)
SELECT
  iz.total_iz_visits,
  iz.fully_immunized,
  iz.immunization_coverage_pct,
  iz.penta1_total,
  iz.penta3_total,
  iz.penta3_coverage_pct,
  iz.penta_dropout_pct,
  iz.iz_defaulters,
  preg.total_pregnancies,
  preg.anc_4plus_count,
  preg.anc_4plus_pct,
  preg.anc_defaulters,
  preg.anc_defaulter_pct,
  preg.danger_sign_cases,
  pnc.total_pnc_visits,
  pnc.pnc_upto_date,
  pnc.pnc_coverage_pct,
  pnc.maternal_deaths,
  fp.fp_clients,
  fp.on_fp,
  ROUND(100.0 * fp.on_fp / NULLIF(pop.total_wra_pop, 0), 1) AS mcpr_pct,
  fp.fp_refills,
  chw.active_chws,
  pop.total_u2_pop,
  pop.total_u5_pop,
  pop.total_wra_pop,
  hv.total_home_visits,
  hv.households_visited,
  sup.avg_supervision_score,
  sup.high_performers,
  sup.needs_support
FROM
  iz,
  preg,
  pnc,
  fp,
  pop,
  chw,
  hv,
  sup
