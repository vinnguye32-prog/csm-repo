-- ============================================================
-- File: 03_procedures.sql
-- Purpose:
--   Stored procedures used for parameterized analytics.
-- Notes:
--   Procedure returns a result set (Databricks SQL procedure syntax).
-- ============================================================

-- ------------------------------------------------------------
-- Procedure: top10_contract_by_program
-- Purpose:
--   Return top 10 contracts by duration for an input program type.
-- Input:
--   program_name: Entertainment | Kids | Movies | Sports | TV_Channel
-- ------------------------------------------------------------
CREATE OR REPLACE PROCEDURE workspace.csm_project.top10_contract_by_program
(
    IN program_name STRING COMMENT 'Entertainment | Kids | Movies | Sports | TV_Channel'
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  SELECT
    Contract,
    upper(trim(program_name)) as Program,
    CASE
      WHEN lower(trim(program_name)) = 'entertainment' THEN Entertainment
      WHEN lower(trim(program_name)) = 'kids'          THEN Kids
      WHEN lower(trim(program_name)) = 'movies'        THEN Movies
      WHEN lower(trim(program_name)) = 'sports'        THEN Sports
      WHEN lower(trim(program_name)) IN ('tv_channel','tv channel','tvchannel','tv') THEN TV_Channel
      ELSE NULL
    END AS Duration
    FROM workspace.csm_project.fact_monthly_customer_interaction
    ORDER BY Duration DESC
    LIMIT 10;
END;
