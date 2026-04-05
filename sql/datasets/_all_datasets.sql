-- ============================================================================
-- All Dashboard Dataset Queries (24 datasets)
-- Community Health Intelligence Platform
-- ============================================================================

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


-- ============================================================================
-- Dataset: ds_monthly_trends
-- Dashboard Dataset ID: 26dac079
-- ============================================================================

SELECT
  reportedm,
  county_clean,
  sub_county_clean,
  COUNT(*) AS registrations,
  SUM(anc_complete) AS anc4_count,
  SUM(is_anc_defaulter) AS defaulter_count,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_pct,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS defaulter_pct
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  county_clean IS NOT NULL
  AND county_clean != ''
GROUP BY
  reportedm,
  county_clean,
  sub_county_clean
ORDER BY
  reportedm


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


-- ============================================================================
-- Dataset: ds_funnel_stages
-- Dashboard Dataset ID: 53809833
-- ============================================================================

-- Dataset: ds_funnel_stages
-- Unpivoted funnel stages for bar chart visualization
WITH totals AS (
  SELECT
    COUNT(*)                      AS total_registered,
    SUM(stage_2_anc1)             AS reached_anc1,
    SUM(stage_3_anc2)             AS reached_anc2,
    SUM(stage_4_anc3)             AS reached_anc3,
    SUM(stage_5_anc4)             AS reached_anc4,
    SUM(stage_6_delivery)         AS reached_delivery,
    SUM(stage_7_pnc)              AS reached_pnc,
    SUM(stage_8_fp)               AS reached_fp
  FROM community_health_intelligence.gold.fact_pregnancy_journey
)
SELECT stage_order, stage_label, women_count,
  ROUND(100.0 * women_count / MAX(women_count) OVER(), 1) AS pct_of_registered
FROM (
  SELECT 1 AS stage_order, 'Stage 1: Registered'         AS stage_label, total_registered    AS women_count FROM totals UNION ALL
  SELECT 2,                'Stage 2: ANC1 initiated',                     reached_anc1        FROM totals UNION ALL
  SELECT 3,                'Stage 3: ANC2 attended',                      reached_anc2        FROM totals UNION ALL
  SELECT 4,                'Stage 4: ANC3 attended',                      reached_anc3        FROM totals UNION ALL
  SELECT 5,                'Stage 5: ANC4+ complete',                     reached_anc4        FROM totals UNION ALL
  SELECT 6,                'Stage 6: Delivery reached',                   reached_delivery    FROM totals UNION ALL
  SELECT 7,                'Stage 7: PNC contacted',                      reached_pnc         FROM totals UNION ALL
  SELECT 8,                'Stage 8: FP postpartum',                      reached_fp          FROM totals
)
ORDER BY stage_order


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


-- ============================================================================
-- Dataset: ds_chw_performance
-- Dashboard Dataset ID: edbdd54b
-- ============================================================================

SELECT
  c.chw_name,
  c.community_unit,
  c.sub_county_name,
  c.county_name,
  s.overall_score_pct,
  s.immunization_score_pct,
  s.pregnancy_score_pct,
  s.nutrition_score_pct,
  s.malaria_score_pct,
  s.fp_score_pct,
  s.newborn_score_pct,
  s.wash_score_pct,
  s.has_all_tools,
  s.has_ppe,
  s.visit_count,
  s.reportedm,
  COALESCE(hv.total_home_visits, 0)                                  AS total_home_visits,
  CASE
    WHEN s.overall_score_pct >= 80 THEN 'High performer'
    WHEN s.overall_score_pct >= 60 THEN 'Meeting expectations'
    WHEN s.overall_score_pct >= 40 THEN 'Below average'
    ELSE 'Needs support' END                                         AS performance_tier,
  CASE WHEN s.has_all_tools = 1 AND s.has_ppe = 1 THEN 'Fully equipped'
       WHEN s.has_all_tools = 1 OR s.has_ppe = 1  THEN 'Partially equipped'
       ELSE 'Not equipped' END                                       AS equipment_status
FROM community_health_intelligence.gold.fact_supervision s
JOIN community_health_intelligence.gold.dim_chw c
  ON s.chw_uuid = c.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS total_home_visits
  FROM community_health_intelligence.gold.fact_home_visit
  GROUP BY chw_uuid
) hv ON s.chw_uuid = hv.chw_uuid
WHERE s.overall_score_pct IS NOT NULL
ORDER BY s.overall_score_pct DESC


-- ============================================================================
-- Dataset: ds_supervision_domain_scores
-- Dashboard Dataset ID: 6254a886
-- ============================================================================

-- Unpivoted domain scores for radar/heatmap
WITH scores AS (
  SELECT
    c.chw_name,
    c.sub_county_name,
    c.county_name,
    s.overall_score_pct,
    s.immunization_score_pct,
    s.pregnancy_score_pct,
    s.nutrition_score_pct,
    s.malaria_score_pct,
    s.fp_score_pct,
    s.newborn_score_pct,
    s.wash_score_pct
  FROM community_health_intelligence.gold.fact_supervision s
  JOIN community_health_intelligence.gold.dim_chw c ON s.chw_uuid = c.chw_uuid
  WHERE s.overall_score_pct IS NOT NULL
)
SELECT domain, ROUND(AVG(score), 1) AS avg_score,
  ROUND(MIN(score), 1) AS min_score,
  ROUND(MAX(score), 1) AS max_score,
  COUNT(*) AS chw_count
FROM (
  SELECT 'Immunization'   AS domain, immunization_score_pct AS score FROM scores
  UNION ALL
  SELECT 'Pregnancy',              pregnancy_score_pct     FROM scores
  UNION ALL
  SELECT 'Nutrition',              nutrition_score_pct     FROM scores
  UNION ALL
  SELECT 'Malaria',                malaria_score_pct       FROM scores
  UNION ALL
  SELECT 'Family Planning',        fp_score_pct            FROM scores
  UNION ALL
  SELECT 'Newborn Care',           newborn_score_pct       FROM scores
  UNION ALL
  SELECT 'WASH',                   wash_score_pct          FROM scores
)
WHERE score IS NOT NULL
GROUP BY domain
ORDER BY avg_score DESC


-- ============================================================================
-- Dataset: ds_chw_kpis
-- Dashboard Dataset ID: 8d3a8d33
-- ============================================================================

SELECT
  COUNT(DISTINCT s.chw_uuid)                                        AS total_supervised,
  ROUND(AVG(s.overall_score_pct), 1)                                AS avg_overall_score,
  SUM(CASE WHEN s.overall_score_pct >= 80 THEN 1 ELSE 0 END)        AS high_performers,
  SUM(CASE WHEN s.overall_score_pct >= 60
        AND s.overall_score_pct < 80 THEN 1 ELSE 0 END)             AS meeting_expectations,
  SUM(CASE WHEN s.overall_score_pct < 40 THEN 1 ELSE 0 END)         AS needs_support,
  SUM(s.has_all_tools)                                              AS fully_equipped,
  SUM(s.has_ppe)                                                    AS has_ppe,
  ROUND(100.0*SUM(s.has_all_tools)/NULLIF(COUNT(*),0),1)            AS tools_readiness_pct,
  ROUND(100.0*SUM(s.has_ppe)/NULLIF(COUNT(*),0),1)                  AS ppe_readiness_pct,
  SUM(hv.total_home_visits)                                         AS total_home_visits,
  ROUND(AVG(hv.total_home_visits),0)                                AS avg_visits_per_chw
FROM community_health_intelligence.gold.fact_supervision s
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS total_home_visits
  FROM community_health_intelligence.gold.fact_home_visit
  GROUP BY chw_uuid
) hv ON s.chw_uuid = hv.chw_uuid
WHERE s.overall_score_pct IS NOT NULL


-- ============================================================================
-- Dataset: ds_coverage_gaps
-- Dashboard Dataset ID: eb770221
-- ============================================================================

SELECT
  c.county_name,
  c.sub_county_name,
  c.community_unit,
  c.chw_name,
  c.chw_uuid,
  COALESCE(pop.u2_pop, 0)                                           AS u2_pop,
  COALESCE(pop.u5_pop, 0)                                           AS u5_pop,
  COALESCE(pop.wra_pop, 0)                                          AS wra_pop,
  COALESCE(iz.iz_visits, 0)                                         AS iz_visits,
  COALESCE(iz.fully_immunized, 0)                                   AS fully_immunized,
  COALESCE(pop.u5_pop, 0) - COALESCE(iz.iz_visits, 0)              AS iz_unserved,
  ROUND(100.0*COALESCE(iz.fully_immunized,0)
    /NULLIF(COALESCE(pop.u5_pop,0),0),1)                            AS iz_coverage_pct,
  COALESCE(fp.on_fp, 0)                                             AS fp_on_fp,
  COALESCE(pop.wra_pop,0) - COALESCE(fp.on_fp,0)                   AS fp_unserved_wra,
  ROUND(100.0*COALESCE(fp.on_fp,0)
    /NULLIF(COALESCE(pop.wra_pop,0),0),1)                           AS mcpr_pct,
  COALESCE(hv.home_visits, 0)                                       AS home_visits,
  ROUND(100.0*COALESCE(hv.home_visits,0)
    /NULLIF(COALESCE(pop.u5_pop,0),0),1)                            AS home_visit_coverage_pct
FROM community_health_intelligence.gold.dim_chw c
LEFT JOIN (
  SELECT chw_uuid,
    SUM(u5_pop) AS u5_pop, SUM(u2_pop) AS u2_pop, SUM(wra_pop) AS wra_pop
  FROM community_health_intelligence.gold.fact_population GROUP BY chw_uuid
) pop ON c.chw_uuid = pop.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, COUNT(*) AS iz_visits, SUM(is_fully_immunized) AS fully_immunized
  FROM community_health_intelligence.gold.fact_immunization GROUP BY chw_uuid
) iz ON c.chw_uuid = iz.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(is_on_fp) AS on_fp
  FROM community_health_intelligence.gold.fact_family_planning GROUP BY chw_uuid
) fp ON c.chw_uuid = fp.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(visit_count) AS home_visits
  FROM community_health_intelligence.gold.fact_home_visit GROUP BY chw_uuid
) hv ON c.chw_uuid = hv.chw_uuid
WHERE c.sub_county_name IS NOT NULL AND c.sub_county_name != ''
ORDER BY iz_coverage_pct ASC


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


-- ============================================================================
-- Dataset: ds_top10_gaps
-- Dashboard Dataset ID: 02d71686
-- ============================================================================

SELECT
  c.sub_county_name,
  c.county_name,
  SUM(COALESCE(pop.u5_pop,0))                                       AS u5_pop,
  SUM(COALESCE(iz.iz_visits,0))                                     AS iz_visits,
  SUM(COALESCE(pop.u5_pop,0)) - SUM(COALESCE(iz.iz_visits,0))      AS iz_unserved,
  ROUND(100.0*SUM(COALESCE(iz.fully_immunized,0))
    /NULLIF(SUM(COALESCE(pop.u5_pop,0)),0),1)                       AS iz_coverage_pct,
  SUM(COALESCE(pop.wra_pop,0))                                      AS wra_pop,
  SUM(COALESCE(fp.on_fp,0))                                         AS fp_on_fp,
  SUM(COALESCE(pop.wra_pop,0)) - SUM(COALESCE(fp.on_fp,0))         AS fp_unserved,
  ROUND(100.0*SUM(COALESCE(fp.on_fp,0))
    /NULLIF(SUM(COALESCE(pop.wra_pop,0)),0),1)                      AS mcpr_pct,
  COUNT(DISTINCT c.chw_uuid)                                        AS chw_count
FROM community_health_intelligence.gold.dim_chw c
LEFT JOIN (
  SELECT chw_uuid,
    SUM(u5_pop) AS u5_pop, SUM(wra_pop) AS wra_pop
  FROM community_health_intelligence.gold.fact_population
  GROUP BY chw_uuid
) pop ON c.chw_uuid = pop.chw_uuid
LEFT JOIN (
  SELECT chw_uuid,
    COUNT(*) AS iz_visits,
    SUM(is_fully_immunized) AS fully_immunized
  FROM community_health_intelligence.gold.fact_immunization
  GROUP BY chw_uuid
) iz ON c.chw_uuid = iz.chw_uuid
LEFT JOIN (
  SELECT chw_uuid, SUM(is_on_fp) AS on_fp
  FROM community_health_intelligence.gold.fact_family_planning
  GROUP BY chw_uuid
) fp ON c.chw_uuid = fp.chw_uuid
WHERE c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
  AND c.county_name IS NOT NULL
GROUP BY c.sub_county_name, c.county_name
ORDER BY iz_unserved DESC
LIMIT 10


-- ============================================================================
-- Dataset: ds_anc_defaulters_subcounty
-- Dashboard Dataset ID: anc_defaulters_subcounty
-- ============================================================================

SELECT
  sub_county_clean AS sub_county_name,
  county_clean AS county_name,
  COUNT(*) AS total_pregnancies,
  SUM(is_anc_defaulter) AS anc_defaulters,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_rate
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  sub_county_clean IS NOT NULL
  AND sub_county_clean != ''
GROUP BY
  sub_county_clean,
  county_clean
ORDER BY
  anc_defaulter_rate DESC


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


-- ============================================================================
-- Dataset: ds_anc4_subcounty
-- Dashboard Dataset ID: anc4_subcounty
-- ============================================================================

SELECT
  sub_county_clean AS sub_county_name,
  county_clean AS county_name,
  COUNT(*) AS total_pregnancies,
  SUM(anc_complete) AS anc4_complete,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc4_rate
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  sub_county_clean IS NOT NULL
  AND sub_county_clean != ''
GROUP BY
  sub_county_clean,
  county_clean
ORDER BY
  anc4_rate DESC


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


-- ============================================================================
-- Dataset: ds_exec_maternal
-- Dashboard Dataset ID: exec_maternal
-- ============================================================================

SELECT
  county_clean AS county_name,
  sub_county_clean AS sub_county_name,
  reportedm,
  COUNT(*) AS pregnancy_registrations,
  SUM(anc_complete) AS anc_4plus_count,
  ROUND(100.0 * SUM(anc_complete) / NULLIF(COUNT(*), 0), 1) AS anc_4plus_completion_pct,
  SUM(is_anc_defaulter) AS anc_defaulter_count,
  ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_rate_pct,
  SUM(has_danger_signs) AS danger_sign_count,
  ROUND(100.0 * SUM(has_danger_signs) / NULLIF(COUNT(*), 0), 1) AS danger_sign_rate_pct,
  0 AS muac_red_count,
  SUM(
    CASE
      WHEN has_danger_signs = 1 THEN 1
      ELSE 0
    END
  ) AS referral_count
FROM
  community_health_intelligence.gold.fact_pregnancy
WHERE
  county_clean IS NOT NULL
  AND county_clean != ''
GROUP BY
  county_clean,
  sub_county_clean,
  reportedm


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


-- ============================================================================
-- Dataset: ds_visit_type_breakdown
-- Dashboard Dataset ID: visit_type_breakdown
-- ============================================================================

SELECT
  county_name,
  sub_county_name,
  visit_type,
  visits
FROM
  (
    SELECT
      c.county_name,
      c.sub_county_name,
      'Household Visits' AS visit_type,
      SUM(hv.visit_count) AS visits
    FROM
      community_health_intelligence.gold.dim_chw c
        JOIN community_health_intelligence.gold.fact_home_visit hv
          ON c.chw_uuid = hv.chw_uuid
    WHERE
      c.sub_county_name IS NOT NULL
      AND c.sub_county_name != ''
    GROUP BY
      c.county_name,
      c.sub_county_name
    UNION ALL
    SELECT
      county_clean,
      sub_county_clean,
      'Immunization',
      COUNT(*)
    FROM
      community_health_intelligence.gold.fact_immunization
    WHERE
      sub_county_clean IS NOT NULL
      AND sub_county_clean != ''
    GROUP BY
      county_clean,
      sub_county_clean
    UNION ALL
    SELECT
      county_clean,
      sub_county_clean,
      'ANC Follow-up',
      COUNT(*)
    FROM
      community_health_intelligence.gold.fact_pregnancy
    WHERE
      sub_county_clean IS NOT NULL
      AND sub_county_clean != ''
    GROUP BY
      county_clean,
      sub_county_clean
    UNION ALL
    SELECT
      county_clean,
      sub_county_clean,
      'Defaulter Trace',
      SUM(is_defaulter)
    FROM
      community_health_intelligence.gold.fact_immunization
    WHERE
      sub_county_clean IS NOT NULL
      AND sub_county_clean != ''
    GROUP BY
      county_clean,
      sub_county_clean
  )


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


-- ============================================================================
-- Dataset: ds_visit_trend
-- Dashboard Dataset ID: daily_visit_trend
-- ============================================================================

SELECT
  hv.date_key,
  CONCAT(
    CASE CAST(hv.date_key % 100 AS INT)
      WHEN 1 THEN 'Jan'
      WHEN 2 THEN 'Feb'
      WHEN 3 THEN 'Mar'
      WHEN 4 THEN 'Apr'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'Jun'
      WHEN 7 THEN 'Jul'
      WHEN 8 THEN 'Aug'
      WHEN 9 THEN 'Sep'
      WHEN 10 THEN 'Oct'
      WHEN 11 THEN 'Nov'
      WHEN 12 THEN 'Dec'
    END,
    ' ',
    CAST(CAST(hv.date_key / 100 AS INT) AS STRING)
  ) AS month_label,
  c.county_name,
  c.sub_county_name,
  SUM(hv.visit_count) AS total_visits,
  COUNT(DISTINCT hv.chw_uuid) AS active_chws,
  COUNT(DISTINCT hv.family_id) AS households_reached
FROM
  community_health_intelligence.gold.fact_home_visit hv
    JOIN community_health_intelligence.gold.dim_chw c
      ON hv.chw_uuid = c.chw_uuid
WHERE
  c.sub_county_name IS NOT NULL
  AND c.sub_county_name != ''
GROUP BY
  hv.date_key,
  c.county_name,
  c.sub_county_name
ORDER BY
  hv.date_key


-- ============================================================================
-- Dataset: ds_maternal_subcounty_perf
-- Dashboard Dataset ID: maternal_subcounty_perf
-- ============================================================================

SELECT
  j.sub_county_clean AS sub_county_name,
  j.county_clean AS county_name,
  COUNT(*) AS total_registered,
  SUM(j.stage_2_anc1) AS reached_anc1,
  SUM(j.stage_5_anc4) AS reached_anc4,
  ROUND(100.0 * SUM(j.stage_2_anc1) / NULLIF(COUNT(*), 0), 1) AS anc1_pct,
  ROUND(100.0 * SUM(j.stage_5_anc4) / NULLIF(COUNT(*), 0), 1) AS anc4_pct,
  COALESCE(p.anc_defaulter_rate, 0) AS defaulter_pct
FROM
  community_health_intelligence.gold.fact_pregnancy_journey j
    LEFT JOIN (
      SELECT
        sub_county_clean,
        ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS anc_defaulter_rate
      FROM
        community_health_intelligence.gold.fact_pregnancy
      WHERE
        sub_county_clean IS NOT NULL
        AND sub_county_clean != ''
      GROUP BY
        sub_county_clean
    ) p
      ON j.sub_county_clean = p.sub_county_clean
WHERE
  j.sub_county_clean IS NOT NULL
  AND j.sub_county_clean != ''
GROUP BY
  j.sub_county_clean,
  j.county_clean,
  p.anc_defaulter_rate
ORDER BY
  anc1_pct DESC


-- ============================================================================
-- Dataset: ds_county_comparison_bars
-- Dashboard Dataset ID: county_comparison_bars
-- ============================================================================

WITH county_maternal AS (
  SELECT
    county_clean,
    ROUND(100.0 * SUM(stage_2_anc1) / NULLIF(COUNT(*), 0), 1) AS anc1_rate,
    ROUND(100.0 * SUM(stage_5_anc4) / NULLIF(COUNT(*), 0), 1) AS anc4_rate,
    ROUND(100.0 * SUM(stage_6_delivery) / NULLIF(COUNT(*), 0), 1) AS delivery_rate,
    ROUND(100.0 * SUM(stage_7_pnc) / NULLIF(COUNT(*), 0), 1) AS pnc1_rate
  FROM
    community_health_intelligence.gold.fact_pregnancy_journey
  WHERE
    county_clean IS NOT NULL
    AND county_clean != ''
    AND county_clean != 'UNKNOWN'
  GROUP BY
    county_clean
),
county_def AS (
  SELECT
    county_clean,
    ROUND(100.0 * SUM(is_anc_defaulter) / NULLIF(COUNT(*), 0), 1) AS defaulter_rate
  FROM
    community_health_intelligence.gold.fact_pregnancy
  WHERE
    county_clean IS NOT NULL
    AND county_clean != ''
    AND county_clean != 'UNKNOWN'
  GROUP BY
    county_clean
),
county_fp AS (
  SELECT
    c.county_name AS county_clean,
    ROUND(
      100.0 * SUM(COALESCE(fp.on_fp, 0)) / NULLIF(SUM(COALESCE(pop.wra_pop, 0)), 0),
      1
    ) AS mcpr_pct
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
  WHERE
    c.county_name IS NOT NULL
    AND c.county_name != ''
  GROUP BY
    c.county_name
)
SELECT
  county_clean AS county,
  metric,
  value,
  sort_order
FROM
  (
    SELECT
      m.county_clean,
      'ANC1 Rate' AS metric,
      m.anc1_rate AS value,
      1 AS sort_order
    FROM
      county_maternal m
    UNION ALL
    SELECT
      m.county_clean,
      'ANC4+ Rate',
      m.anc4_rate,
      2
    FROM
      county_maternal m
    UNION ALL
    SELECT
      d.county_clean,
      'Defaulter Rate',
      d.defaulter_rate,
      3
    FROM
      county_def d
    UNION ALL
    SELECT
      m.county_clean,
      'Skilled Delivery',
      m.delivery_rate,
      4
    FROM
      county_maternal m
    UNION ALL
    SELECT
      m.county_clean,
      'PNC1 Coverage',
      m.pnc1_rate,
      5
    FROM
      county_maternal m
    UNION ALL
    SELECT
      f.county_clean,
      'mCPR',
      f.mcpr_pct,
      6
    FROM
      county_fp f
  )
ORDER BY
  sort_order,
  county_clean


