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
