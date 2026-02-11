-- ============================================================
-- File: 91_validation_checks.sql
-- Purpose:
--   Quick data quality checks for portfolio demonstration.
-- ============================================================

-- Row counts
SELECT COUNT(*) AS fact_interaction_rows
FROM workspace.csm_project.fact_monthly_customer_interaction;

SELECT COUNT(*) AS fact_behavior_rows
FROM workspace.csm_project.fact_monthly_customer_behavior;

-- Dimension uniqueness checks
SELECT COUNT(*) AS dim_user_rows, COUNT(DISTINCT user_id) AS dim_user_distinct
FROM workspace.csm_project.dim_user;

SELECT COUNT(*) AS dim_contract_rows, COUNT(DISTINCT contract) AS dim_contract_distinct
FROM workspace.csm_project.dim_contract;

-- Null checks
SELECT
  SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id_rows
FROM workspace.csm_project.fact_monthly_customer_behavior;

SELECT
  SUM(CASE WHEN Contract IS NULL THEN 1 ELSE 0 END) AS null_contract_rows
FROM workspace.csm_project.fact_monthly_customer_interaction;
