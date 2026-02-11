-- ============================================================
-- File: 04_views.sql
-- Purpose:
--   Create curated views used for BI and dashboarding.
-- Notes:
--   This file should ONLY contain CREATE VIEW statements.
-- ============================================================

-- ============================================================
-- Active Days Distribution
-- Purpose:
--   Bucket each contract into an Active_Days bin, then count
--   distinct contracts per bin for distribution analysis.
-- Notes:
--   - 'Unknown' captures NULL Active_Days.
--   - Bins are ordered manually to keep a logical progression.
-- ============================================================
CREATE OR REPLACE VIEW workspace.csm_project.vw_contract_active_days_distribution AS
WITH binned AS (
  SELECT
    CASE
      WHEN Active_Days IS NULL THEN 'Unknown'
      WHEN Active_Days >= 0  AND Active_Days < 5  THEN '0-5'
      WHEN Active_Days >= 5  AND Active_Days < 10 THEN '5-10'
      WHEN Active_Days >= 10 AND Active_Days < 15 THEN '10-15'
      WHEN Active_Days >= 15 AND Active_Days < 20 THEN '15-20'
      WHEN Active_Days >= 20 AND Active_Days < 25 THEN '20-25'
      WHEN Active_Days >= 25 AND Active_Days < 30 THEN '25-30'
      ELSE '30'
    END AS active_days_bin,
    Contract
  FROM workspace.csm_project.fact_monthly_customer_interaction
)
SELECT
  active_days_bin,
  COUNT(DISTINCT Contract) AS distinct_contract_count
FROM binned
GROUP BY active_days_bin
ORDER BY
  CASE active_days_bin
    WHEN '0-5'   THEN 1
    WHEN '5-10'  THEN 2
    WHEN '10-15' THEN 3
    WHEN '15-20' THEN 4
    WHEN '20-25' THEN 5
    WHEN '25-30' THEN 6
    WHEN '30'    THEN 7
    ELSE 8
  END;

-- ============================================================
-- Program Customer Analytics
-- Purpose:
--   For each program type, compute total duration + distinct
--   contracts + avg active days among watchers.
-- Notes:
--   - UNION ALL stacks per-program aggregates.
--   - Filters to duration > 0 to exclude non-watchers.
-- ============================================================
CREATE OR REPLACE VIEW workspace.csm_project.vw_program_duration_contract_stats AS

SELECT
  'Entertainment' AS program_name,
  SUM(Entertainment) AS total_duration,
  SUM(Entertainment)/3600 AS total_duration_hours,
  COUNT(DISTINCT Contract) AS distinct_contract_count,
  AVG(Active_Days) AS avg_active_days
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE Entertainment > 0

UNION ALL

SELECT
  'Kids' AS program_name,
  SUM(Kids) AS total_duration,
  SUM(Kids)/3600 AS total_duration_hours,
  COUNT(DISTINCT Contract) AS distinct_contract_count,
  AVG(Active_Days) AS avg_active_days
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE Kids > 0

UNION ALL

SELECT
  'Movies' AS program_name,
  SUM(Movies) AS total_duration,
  SUM(Movies)/3600 AS total_duration_hours,
  COUNT(DISTINCT Contract) AS distinct_contract_count,
  AVG(Active_Days) AS avg_active_days
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE Movies > 0

UNION ALL

SELECT
  'Sports' AS program_name,
  SUM(Sports) AS total_duration,
  SUM(Sports)/3600 AS total_duration_hours,
  COUNT(DISTINCT Contract) AS distinct_contract_count,
  AVG(Active_Days) AS avg_active_days
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE Sports > 0

UNION ALL

SELECT
  'TV_Channel' AS program_name,
  SUM(TV_Channel) AS total_duration,
  SUM(TV_Channel)/3600 AS total_duration_hours,
  COUNT(DISTINCT Contract) AS distinct_contract_count,
  AVG(Active_Days) AS avg_active_days
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE TV_Channel > 0;

-- ============================================================
-- Customer Taste vs Contract Count
-- Purpose:
--   Count distinct contracts by customer_taste.
-- ============================================================
CREATE OR REPLACE VIEW workspace.csm_project.vw_customer_taste_contract_devices AS
SELECT
  customer_taste,
  COUNT(DISTINCT Contract) AS distinct_contract_count
FROM workspace.csm_project.fact_monthly_customer_interaction
GROUP BY customer_taste
ORDER BY 2 DESC;
