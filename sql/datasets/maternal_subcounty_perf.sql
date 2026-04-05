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
