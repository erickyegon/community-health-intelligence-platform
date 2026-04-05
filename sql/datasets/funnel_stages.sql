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
