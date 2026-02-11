-- ============================================================
-- File: 01_dimensions.sql
-- Purpose:
--   Create dimension tables used for modeling and BI joins.
-- Notes:
--   Re-runnable via CREATE OR REPLACE.
-- ============================================================

-- ------------------------------------------------------------
-- dim_user: all distinct non-null user_id values
-- Source: fact_monthly_customer_behavior
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.csm_project.dim_user AS
SELECT DISTINCT user_id
FROM workspace.csm_project.fact_monthly_customer_behavior
WHERE user_id IS NOT NULL;

-- ------------------------------------------------------------
-- dim_contract: all distinct non-null contract values
-- Source: fact_monthly_customer_interaction
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE workspace.csm_project.dim_contract AS
SELECT DISTINCT contract
FROM workspace.csm_project.fact_monthly_customer_interaction
WHERE contract IS NOT NULL;
