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
