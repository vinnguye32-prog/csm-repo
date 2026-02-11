-- ============================================================
-- File: 90_analysis_examples.sql
-- Purpose:
--   Demo queries to showcase outputs (NOT production DDL).
--   Keep exploration and examples here, separate from CREATE VIEW/TABLE.
-- ============================================================

-- Example: call procedure (top 10 by TV)
CALL workspace.csm_project.top10_contract_by_program('tv');

-- View checks / exploration
SELECT * FROM workspace.csm_project.vw_contract_active_days_distribution;

SELECT * FROM workspace.csm_project.vw_program_duration_contract_stats;

SELECT * FROM workspace.csm_project.vw_customer_taste_contract_devices;

-- Simple KPI: Average TotalDevices
SELECT AVG(TotalDevices)
FROM workspace.csm_project.fact_monthly_customer_interaction;

-- Sample records (demo)
SELECT *
FROM workspace.csm_project.fact_monthly_customer_behavior
LIMIT 100;

-- ------------------------------------------------------------
-- Genre change analysis 
-- ------------------------------------------------------------

SELECT
  ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN Trending_Type <> 'Unchanged' THEN user_id END) / COUNT(DISTINCT user_id),
    2
  ) AS percent_unchanged
FROM workspace.csm_project.fact_monthly_customer_behavior;

SELECT
  Previous_Genre,
  COUNT(DISTINCT user_id) AS changed_user_count
FROM workspace.csm_project.fact_monthly_customer_behavior
WHERE Trending_Type <> 'Unchanged'
GROUP BY Previous_Genre
ORDER BY changed_user_count DESC;
