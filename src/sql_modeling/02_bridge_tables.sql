-- ============================================================
-- File: 02_bridge_tables.sql
-- Purpose:
--   Create bridge table linking users and contracts.
-- Notes:
--   Structure-only table to support demos / relationship modeling.
-- ============================================================

-- Demo bridge table linking users and contracts
CREATE OR REPLACE TABLE workspace.csm_project.user_contract_details
(
    user_id     STRING,
    contract_id STRING,
    start_date  DATE,
    end_date    DATE,
    is_primary  BOOLEAN
)
USING DELTA;

-- ------------------------------------------------------------
-- Randomize Data for Bridge Table
-- (Intentionally left as placeholder in your original script)
-- Keep any synthetic insert logic in a separate file later:
-- e.g., 92_seed_demo_data.sql
-- ------------------------------------------------------------
