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
