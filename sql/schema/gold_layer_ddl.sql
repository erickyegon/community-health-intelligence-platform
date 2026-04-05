-- ============================================================================
-- Gold Layer Schema DDL
-- Catalog: community_health_intelligence
-- Schema: gold
-- ============================================================================

-- Table: dim_chw
CREATE TABLE community_health_intelligence.gold.dim_chw (
  chw_uuid STRING,
  chw_area_uuid STRING,
  chw_area_name STRING,
  chw_name STRING,
  chw_phone STRING,
  cha_name STRING,
  community_unit STRING,
  county_name STRING,
  sub_county_name STRING)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_name, sub_county_name)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: dim_facility
CREATE TABLE community_health_intelligence.gold.dim_facility (
  facility_code STRING,
  facility_name STRING,
  county_name STRING,
  sub_county_name STRING)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_name, sub_county_name)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: dim_geography
CREATE TABLE community_health_intelligence.gold.dim_geography (
  geo_key STRING,
  county_name STRING,
  sub_county_name STRING,
  community_unit STRING)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_name, sub_county_name)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_family_planning
CREATE TABLE community_health_intelligence.gold.fact_family_planning (
  fact_key STRING,
  date_key INT,
  chw_uuid STRING,
  reportedm STRING,
  patient_id STRING,
  age_years INT,
  sex STRING,
  county_clean STRING,
  sub_county_clean STRING,
  county_qc_flag STRING,
  is_on_fp INT,
  fp_method STRING,
  refilled_today INT,
  coc_cycles INT,
  pop_cycles INT,
  condom_pieces INT,
  has_side_effects INT,
  is_referral INT,
  source_form STRING,
  lat DOUBLE,
  lon DOUBLE,
  fp_method_category STRING)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_home_visit
CREATE TABLE community_health_intelligence.gold.fact_home_visit (
  fact_key STRING,
  date_key INT,
  chw_uuid STRING,
  reportedm STRING,
  county_clean STRING,
  sub_county_clean STRING,
  family_id STRING,
  visit_count INT)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_immunization
CREATE TABLE community_health_intelligence.gold.fact_immunization (
  fact_key STRING,
  date_key INT,
  chw_uuid STRING,
  facility_code STRING,
  patient_id STRING,
  reportedm STRING,
  county_clean STRING,
  sub_county_clean STRING,
  community_unit_clean STRING,
  county_qc_flag STRING,
  age_months INT,
  vaccines_due INT,
  is_fully_immunized INT,
  is_defaulter INT,
  is_referred INT,
  bcg INT,
  opv0 INT,
  opv1 INT,
  opv2 INT,
  opv3 INT,
  penta1 INT,
  penta2 INT,
  penta3 INT,
  pcv1 INT,
  pcv3 INT,
  measles_9m INT,
  measles_18m INT,
  vitamin_a INT,
  lat DOUBLE,
  lon DOUBLE)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_pnc
CREATE TABLE community_health_intelligence.gold.fact_pnc (
  fact_key STRING,
  date_key INT,
  chw_uuid STRING,
  reportedm STRING,
  patient_id STRING,
  age_years INT,
  sex STRING,
  county_clean STRING,
  sub_county_clean STRING,
  county_qc_flag STRING,
  delivery_date DATE,
  place_of_delivery STRING,
  days_since_delivery INT,
  pnc_visit_count INT,
  pnc_upto_date INT,
  condition_of_the_mother STRING,
  needs_danger_followup INT,
  is_maternal_death INT,
  started_fp INT,
  newborn_visits INT,
  lat DOUBLE,
  lon DOUBLE)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_population
CREATE TABLE community_health_intelligence.gold.fact_population (
  report_month STRING,
  chw_uuid STRING,
  chw_area_uuid STRING,
  county_clean STRING,
  sub_county_clean STRING,
  community_unit_clean STRING,
  u2_pop INT,
  u5_pop INT,
  wra_pop INT)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_pregnancy
CREATE TABLE community_health_intelligence.gold.fact_pregnancy (
  fact_key STRING,
  date_key INT,
  reportedm STRING,
  patient_id STRING,
  age_years INT,
  county_clean STRING,
  sub_county_clean STRING,
  county_qc_flag STRING,
  chu_code INT,
  chu_name STRING,
  has_started_anc INT,
  anc_visits INT,
  next_anc_date DATE,
  edd DATE,
  has_danger_signs INT,
  muac_color STRING,
  on_iron_folate INT,
  is_anc_defaulter INT,
  is_referred INT,
  anc_complete INT,
  muac_risk STRING,
  source_form STRING)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_pregnancy_journey
CREATE TABLE community_health_intelligence.gold.fact_pregnancy_journey (
  registration_uuid STRING,
  patient_id STRING,
  reportedm STRING,
  registered_at TIMESTAMP,
  age_years INT,
  edd DATE,
  chu_code INT,
  chu_name STRING,
  county_clean STRING,
  sub_county_clean STRING,
  muac_worst STRING,
  had_danger_signs INT,
  ever_on_iron_folate INT,
  total_anc_visits INT,
  delivery_date DATE,
  days_to_pnc INT,
  total_pnc INT,
  pnc_upto_date INT,
  fp_at_pnc INT,
  is_maternal_death INT,
  facility_delivery INT,
  on_fp INT,
  fp_method_chosen STRING,
  stage_1_registered INT,
  stage_2_anc1 INT,
  stage_3_anc2 INT,
  stage_4_anc3 INT,
  stage_5_anc4 INT,
  stage_6_delivery INT,
  stage_7_pnc INT,
  stage_8_fp INT,
  pnc_within_48hrs INT,
  days_reg_to_anc1 INT,
  funnel_stage_reached INT,
  dropout_stage STRING,
  maternal_age_group STRING,
  gold_loaded_at TIMESTAMP)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- Table: fact_supervision
CREATE TABLE community_health_intelligence.gold.fact_supervision (
  fact_key STRING,
  date_key INT,
  chw_uuid STRING,
  reportedm STRING,
  county_clean STRING,
  sub_county_clean STRING,
  county_qc_flag STRING,
  supervisor_name STRING,
  last_visit_date DATE,
  visit_count INT,
  overall_score_pct DOUBLE,
  immunization_score_pct DOUBLE,
  pregnancy_score_pct DOUBLE,
  nutrition_score_pct DOUBLE,
  malaria_score_pct DOUBLE,
  fp_score_pct DOUBLE,
  newborn_score_pct DOUBLE,
  wash_score_pct DOUBLE,
  has_all_tools INT,
  has_ppe INT)
USING delta
COLLATION 'UTF8_BINARY'
WITH ROW FILTER `community_health_intelligence`.`gold`.`subcounty_access_filter` ON (county_clean, sub_county_clean)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
;

-- View: mv_family_planning
CREATE VIEW gold.mv_family_planning (
  county_clean,
  sub_county_clean,
  reportedm,
  chw_uuid,
  fp_method,
  fp_method_category,
  fp_clients,
  on_fp_count,
  mcpr_pct,
  refill_count,
  refill_rate_pct,
  side_effect_count)
WITH SCHEMA COMPENSATION
AS SELECT f.county_clean, f.sub_county_clean, f.reportedm, f.chw_uuid,
  f.fp_method, f.fp_method_category,
  COUNT(DISTINCT f.patient_id)                                        AS fp_clients,
  SUM(f.is_on_fp)                                                     AS on_fp_count,
  ROUND(100.0*SUM(f.is_on_fp)/NULLIF(SUM(p.wra_pop),0),2)            AS mcpr_pct,
  SUM(f.refilled_today)                                               AS refill_count,
  ROUND(100.0*SUM(f.refilled_today)/NULLIF(COUNT(*),0),2)             AS refill_rate_pct,
  SUM(f.has_side_effects)                                             AS side_effect_count
FROM community_health_intelligence.gold.fact_family_planning f
LEFT JOIN (
  SELECT chw_uuid, SUM(wra_pop) AS wra_pop
  FROM community_health_intelligence.gold.fact_population GROUP BY chw_uuid
) p ON f.chw_uuid = p.chw_uuid
GROUP BY ALL
;

-- View: mv_immunization
CREATE VIEW gold.mv_immunization (
  county_clean,
  sub_county_clean,
  community_unit_clean,
  reportedm,
  chw_uuid,
  county_qc_flag,
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
  referral_count)
WITH SCHEMA COMPENSATION
AS SELECT county_clean, sub_county_clean, community_unit_clean, reportedm, chw_uuid, county_qc_flag,
  COUNT(*)                                                           AS immunization_visits,
  SUM(is_fully_immunized)                                            AS fully_immunized_count,
  ROUND(100.0*SUM(is_fully_immunized)/NULLIF(COUNT(*),0),2)          AS immunization_coverage_pct,
  SUM(is_defaulter)                                                  AS defaulter_count,
  ROUND(100.0*SUM(is_defaulter)/NULLIF(COUNT(*),0),2)                AS defaulter_rate_pct,
  SUM(penta1)                                                        AS penta1_count,
  SUM(penta3)                                                        AS penta3_count,
  ROUND(100.0*(SUM(penta1)-SUM(penta3))/NULLIF(SUM(penta1),0),2)    AS penta_dropout_rate_pct,
  SUM(bcg)                                                           AS bcg_count,
  SUM(measles_9m)                                                    AS measles_9m_count,
  SUM(vitamin_a)                                                     AS vitamin_a_count,
  SUM(is_referred)                                                   AS referral_count
FROM community_health_intelligence.gold.fact_immunization
GROUP BY ALL
;

-- View: mv_maternal_health
CREATE VIEW gold.mv_maternal_health (
  county_clean,
  sub_county_clean,
  reportedm,
  muac_risk,
  source_form,
  pregnancy_registrations,
  anc_4plus_count,
  anc_4plus_completion_pct,
  anc_defaulter_count,
  anc_defaulter_rate_pct,
  danger_sign_count,
  danger_sign_rate_pct,
  iron_folate_coverage_pct,
  muac_red_count,
  referral_count)
WITH SCHEMA COMPENSATION
AS SELECT county_clean, sub_county_clean, reportedm, muac_risk, source_form,
  COUNT(*)                                                            AS pregnancy_registrations,
  SUM(anc_complete)                                                   AS anc_4plus_count,
  ROUND(100.0*SUM(anc_complete)/NULLIF(COUNT(*),0),2)                 AS anc_4plus_completion_pct,
  SUM(is_anc_defaulter)                                               AS anc_defaulter_count,
  ROUND(100.0*SUM(is_anc_defaulter)/NULLIF(COUNT(*),0),2)             AS anc_defaulter_rate_pct,
  SUM(has_danger_signs)                                               AS danger_sign_count,
  ROUND(100.0*SUM(has_danger_signs)/NULLIF(COUNT(*),0),2)             AS danger_sign_rate_pct,
  ROUND(100.0*SUM(on_iron_folate)/NULLIF(COUNT(*),0),2)               AS iron_folate_coverage_pct,
  SUM(CASE WHEN muac_color='red' THEN 1 ELSE 0 END)                   AS muac_red_count,
  SUM(is_referred)                                                    AS referral_count
FROM community_health_intelligence.gold.fact_pregnancy
GROUP BY ALL
;

-- View: mv_supervision
CREATE VIEW gold.mv_supervision (
  county_clean,
  sub_county_clean,
  reportedm,
  chw_uuid,
  performance_tier,
  supervision_visits,
  avg_overall_score_pct,
  avg_immunization_score_pct,
  avg_pregnancy_score_pct,
  avg_fp_score_pct,
  avg_newborn_score_pct,
  avg_wash_score_pct,
  tools_readiness_pct,
  ppe_readiness_pct,
  high_performer_count,
  needs_support_count)
WITH SCHEMA COMPENSATION
AS SELECT county_clean, sub_county_clean, reportedm, chw_uuid,
  CASE WHEN overall_score_pct>=80 THEN 'High performer'
       WHEN overall_score_pct>=60 THEN 'Meeting expectations'
       ELSE 'Needs support' END                                       AS performance_tier,
  COUNT(*)                                                            AS supervision_visits,
  ROUND(AVG(overall_score_pct),2)                                     AS avg_overall_score_pct,
  ROUND(AVG(immunization_score_pct),2)                                AS avg_immunization_score_pct,
  ROUND(AVG(pregnancy_score_pct),2)                                   AS avg_pregnancy_score_pct,
  ROUND(AVG(fp_score_pct),2)                                          AS avg_fp_score_pct,
  ROUND(AVG(newborn_score_pct),2)                                     AS avg_newborn_score_pct,
  ROUND(AVG(wash_score_pct),2)                                        AS avg_wash_score_pct,
  ROUND(100.0*SUM(has_all_tools)/NULLIF(COUNT(*),0),2)                AS tools_readiness_pct,
  ROUND(100.0*SUM(has_ppe)/NULLIF(COUNT(*),0),2)                      AS ppe_readiness_pct,
  SUM(CASE WHEN overall_score_pct>=80 THEN 1 ELSE 0 END)              AS high_performer_count,
  SUM(CASE WHEN overall_score_pct<60  THEN 1 ELSE 0 END)              AS needs_support_count
FROM community_health_intelligence.gold.fact_supervision
GROUP BY ALL
;

-- View: vw_chw_performance
CREATE VIEW gold.vw_chw_performance (
  month_label,
  year,
  chw_uuid,
  chw_name,
  community_unit,
  county_name,
  sub_county_name,
  overall_score_pct,
  immunization_score_pct,
  pregnancy_score_pct,
  nutrition_score_pct,
  malaria_score_pct,
  fp_score_pct,
  newborn_score_pct,
  wash_score_pct,
  has_all_tools,
  has_ppe,
  total_home_visits,
  performance_tier)
WITH SCHEMA COMPENSATION
AS SELECT d.month_label,d.year,s.chw_uuid,c.chw_name,c.community_unit,c.county_name,c.sub_county_name,
  s.overall_score_pct,s.immunization_score_pct,s.pregnancy_score_pct,
  s.nutrition_score_pct,s.malaria_score_pct,s.fp_score_pct,
  s.newborn_score_pct,s.wash_score_pct,s.has_all_tools,s.has_ppe,
  COALESCE(hv.total_home_visits,0) AS total_home_visits,
  CASE WHEN s.overall_score_pct>=80 THEN 'High performer'
       WHEN s.overall_score_pct>=60 THEN 'Meeting expectations'
       ELSE 'Needs support' END AS performance_tier
FROM community_health_intelligence.gold.fact_supervision s
JOIN community_health_intelligence.gold.dim_date d ON s.date_key=d.date_key
JOIN community_health_intelligence.gold.dim_chw c  ON s.chw_uuid=c.chw_uuid
LEFT JOIN (SELECT date_key,chw_uuid,SUM(visit_count) AS total_home_visits FROM community_health_intelligence.gold.fact_home_visit GROUP BY ALL) hv ON s.date_key=hv.date_key AND s.chw_uuid=hv.chw_uuid
;

-- View: vw_coverage_gaps
CREATE VIEW gold.vw_coverage_gaps (
  month_label,
  year,
  county_name,
  sub_county_name,
  community_unit,
  chw_uuid,
  chw_name,
  u2_pop,
  u5_pop,
  wra_pop,
  iz_visits,
  iz_unserved,
  iz_coverage_pct,
  mcpr_pct)
WITH SCHEMA COMPENSATION
AS SELECT d.month_label,d.year,c.county_name,c.sub_county_name,c.community_unit,c.chw_uuid,c.chw_name,
  COALESCE(p.u2_pop,0) AS u2_pop, COALESCE(p.u5_pop,0) AS u5_pop, COALESCE(p.wra_pop,0) AS wra_pop,
  COALESCE(iz.iz_visits,0) AS iz_visits,
  COALESCE(p.u5_pop,0)-COALESCE(iz.iz_visits,0) AS iz_unserved,
  ROUND(100.0*COALESCE(iz.iz_visits,0)/NULLIF(p.u5_pop,0),1) AS iz_coverage_pct,
  ROUND(100.0*COALESCE(fp.on_fp,0)/NULLIF(p.wra_pop,0),1) AS mcpr_pct
FROM community_health_intelligence.gold.dim_chw c CROSS JOIN community_health_intelligence.gold.dim_date d
LEFT JOIN (SELECT chw_uuid,SUM(u2_pop) AS u2_pop,SUM(u5_pop) AS u5_pop,SUM(wra_pop) AS wra_pop FROM community_health_intelligence.gold.fact_population GROUP BY chw_uuid) p ON c.chw_uuid=p.chw_uuid
LEFT JOIN (SELECT date_key,chw_uuid,COUNT(*) AS iz_visits FROM community_health_intelligence.gold.fact_immunization GROUP BY ALL) iz ON d.date_key=iz.date_key AND c.chw_uuid=iz.chw_uuid
LEFT JOIN (SELECT date_key,chw_uuid,SUM(is_on_fp) AS on_fp FROM community_health_intelligence.gold.fact_family_planning GROUP BY ALL) fp ON d.date_key=fp.date_key AND c.chw_uuid=fp.chw_uuid
WHERE iz.iz_visits IS NOT NULL OR fp.on_fp IS NOT NULL
;

-- View: vw_executive_summary
CREATE VIEW gold.vw_executive_summary (
  month_label,
  year,
  quarter_label,
  county_name,
  sub_county_name,
  community_unit,
  chw_name,
  iz_visits,
  immunization_coverage_pct,
  preg_registrations,
  anc_defaulter_pct,
  anc_4plus_pct,
  danger_sign_cases,
  pnc_visits,
  pnc_coverage_pct,
  fp_clients,
  mcpr_pct,
  pop_u2,
  pop_u5,
  pop_wra)
WITH SCHEMA COMPENSATION
AS WITH imm AS (SELECT date_key,chw_uuid,COUNT(*) AS iz_visits,SUM(is_fully_immunized) AS iz_fi,SUM(is_defaulter) AS iz_def FROM community_health_intelligence.gold.fact_immunization GROUP BY ALL),
pnc AS (SELECT date_key,chw_uuid,COUNT(*) AS pnc_visits,SUM(pnc_upto_date) AS pnc_cur FROM community_health_intelligence.gold.fact_pnc GROUP BY ALL),
fp  AS (SELECT date_key,chw_uuid,COUNT(DISTINCT patient_id) AS fp_clients,SUM(is_on_fp) AS on_fp FROM community_health_intelligence.gold.fact_family_planning GROUP BY ALL),
pop AS (SELECT chw_uuid,SUM(u2_pop) AS u2,SUM(u5_pop) AS u5,SUM(wra_pop) AS wra FROM community_health_intelligence.gold.fact_population GROUP BY chw_uuid),
preg AS (SELECT date_key,county_clean,COUNT(*) AS preg_reg,SUM(is_anc_defaulter) AS anc_def,SUM(anc_complete) AS anc_4p,SUM(has_danger_signs) AS danger FROM community_health_intelligence.gold.fact_pregnancy GROUP BY ALL)
SELECT d.month_label,d.year,d.quarter_label,c.county_name,c.sub_county_name,c.community_unit,c.chw_name,
  COALESCE(i.iz_visits,0) AS iz_visits,
  ROUND(100.0*COALESCE(i.iz_fi,0)/NULLIF(i.iz_visits,0),1) AS immunization_coverage_pct,
  COALESCE(p.preg_reg,0) AS preg_registrations,
  ROUND(100.0*COALESCE(p.anc_def,0)/NULLIF(p.preg_reg,0),1) AS anc_defaulter_pct,
  ROUND(100.0*COALESCE(p.anc_4p,0)/NULLIF(p.preg_reg,0),1) AS anc_4plus_pct,
  COALESCE(p.danger,0) AS danger_sign_cases,
  COALESCE(n.pnc_visits,0) AS pnc_visits,
  ROUND(100.0*COALESCE(n.pnc_cur,0)/NULLIF(n.pnc_visits,0),1) AS pnc_coverage_pct,
  COALESCE(f.fp_clients,0) AS fp_clients,
  ROUND(100.0*COALESCE(f.on_fp,0)/NULLIF(po.wra,0),1) AS mcpr_pct,
  COALESCE(po.u2,0) AS pop_u2,COALESCE(po.u5,0) AS pop_u5,COALESCE(po.wra,0) AS pop_wra
FROM community_health_intelligence.gold.dim_date d CROSS JOIN community_health_intelligence.gold.dim_chw c
LEFT JOIN imm i  ON d.date_key=i.date_key AND c.chw_uuid=i.chw_uuid
LEFT JOIN pnc n  ON d.date_key=n.date_key AND c.chw_uuid=n.chw_uuid
LEFT JOIN fp  f  ON d.date_key=f.date_key AND c.chw_uuid=f.chw_uuid
LEFT JOIN pop po ON c.chw_uuid=po.chw_uuid
LEFT JOIN preg p ON d.date_key=p.date_key AND c.county_name=p.county_clean
WHERE i.iz_visits IS NOT NULL OR p.preg_reg IS NOT NULL OR n.pnc_visits IS NOT NULL OR f.on_fp IS NOT NULL
;

-- View: vw_maternal_funnel
CREATE VIEW gold.vw_maternal_funnel (
  county_clean,
  reportedm,
  total_registered,
  reached_anc1,
  reached_anc2,
  reached_anc3,
  reached_anc4,
  reached_delivery,
  reached_pnc,
  reached_fp,
  pct_anc1,
  pct_anc4_complete,
  pct_pnc,
  pct_pnc_48hrs,
  pct_facility_delivery,
  dropout_before_anc1,
  dropout_anc1_anc2,
  dropout_anc2_anc3,
  dropout_anc3_anc4,
  total_danger_signs,
  maternal_deaths)
WITH SCHEMA COMPENSATION
AS SELECT county_clean, reportedm,
  COUNT(*)                                                           AS total_registered,
  SUM(stage_2_anc1)      AS reached_anc1,  SUM(stage_3_anc2) AS reached_anc2,
  SUM(stage_4_anc3)      AS reached_anc3,  SUM(stage_5_anc4) AS reached_anc4,
  SUM(stage_6_delivery)  AS reached_delivery, SUM(stage_7_pnc) AS reached_pnc,
  SUM(stage_8_fp)        AS reached_fp,
  ROUND(100.0*SUM(stage_2_anc1)/NULLIF(COUNT(*),0),1)               AS pct_anc1,
  ROUND(100.0*SUM(stage_5_anc4)/NULLIF(COUNT(*),0),1)               AS pct_anc4_complete,
  ROUND(100.0*SUM(stage_7_pnc)/NULLIF(COUNT(*),0),1)                AS pct_pnc,
  ROUND(100.0*SUM(pnc_within_48hrs)/NULLIF(SUM(stage_7_pnc),0),1)   AS pct_pnc_48hrs,
  ROUND(100.0*SUM(facility_delivery)/NULLIF(SUM(stage_6_delivery),0),1) AS pct_facility_delivery,
  COUNT(*)-SUM(stage_2_anc1)               AS dropout_before_anc1,
  SUM(stage_2_anc1)-SUM(stage_3_anc2)     AS dropout_anc1_anc2,
  SUM(stage_3_anc2)-SUM(stage_4_anc3)     AS dropout_anc2_anc3,
  SUM(stage_4_anc3)-SUM(stage_5_anc4)     AS dropout_anc3_anc4,
  SUM(had_danger_signs)                    AS total_danger_signs,
  SUM(is_maternal_death)                   AS maternal_deaths
FROM community_health_intelligence.gold.fact_pregnancy_journey
GROUP BY ALL
;
