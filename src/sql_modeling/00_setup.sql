-- ============================================================
-- Project: CSM Analytics (Databricks SQL)
-- File: 00_setup.sql
-- Purpose:
--   Set the active catalog/schema for consistent execution.
-- ============================================================

USE CATALOG workspace;

CREATE SCHEMA IF NOT EXISTS csm_project;

USE SCHEMA csm_project;
