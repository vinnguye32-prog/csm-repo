# Project: Customer System Management (CSM) – Data Engineering on Databricks

This project builds a small analytics warehouse on Databricks using two ETL pipelines and a SQL modeling layer (dimensions, bridge table, views, and a stored procedure) to support BI/dashboarding.

---

## Architecture (High Level)

### Data Source/Storage:

Data is placed in S3 bucket to be read from databrick Python scripts.

<img width="1434" height="662" alt="82990" src="https://github.com/user-attachments/assets/c8deaad5-a92c-4517-897b-3d23e9264c1e" />


### ETL Pipelines
1. **`ETL_Most_Searched.py`**  
   Produces: **`workspace.csm_project.fact_monthly_customer_behavior`**  
   Purpose: monthly customer behavior metrics (ex: most searched / trending type / previous genre)

2. **`ETL_30Days.py`**  
   Produces: **`workspace.csm_project.fact_monthly_customer_interaction`**  
   Purpose: monthly customer interaction metrics (ex: program durations by category, active days, devices, customer taste)

### SQL Modeling (Databricks SQL)
Folder: `src/sql_modeling/`

Run order:
1. `src/sql_modeling/00_setup.sql`
2. `src/sql_modeling/01_dimensions.sql`
3. `src/sql_modeling/02_bridge_tables.sql`
4. `src/sql_modeling/03_procedures.sql`
5. `src/sql_modeling/04_views.sql`
6. `src/sql_modeling/90_analysis_examples.sql` (demo queries)
7. `src/sql_modeling/91_validation_checks.sql` (data quality checks)

Key outputs:
- `dim_user` – distinct users derived from behavior fact
- `dim_contract` – distinct contracts derived from interaction fact
- `user_contract_details` – demo bridge table linking user ↔ contract
- `vw_contract_active_days_distribution` – contract counts by active-day bins (histogram input)
- `vw_program_duration_contract_stats` – duration + contract coverage by program category
- `vw_customer_taste_contract_devices` – contract distribution by customer taste
- `top10_contract_by_program(program_name)` – parameterized top-10 query by program duration

<img width="350" height="427" alt="My organization" src="https://github.com/user-attachments/assets/558a856b-fd94-4730-82ec-83dbe1dded95" />


---

## Repository Structure

- `src/customer_behavior/`
  - `ETL_Most_Searched.py` (builds `fact_monthly_customer_behavior`)
  - `mapping.py` (helper mapping logic)
  - `map/` (supporting mapping assets)
- `src/customer_interaction/`
  - `ETL_30Days.py` (builds `fact_monthly_customer_interaction`)
- `src/sql_modeling/`
  - setup, dimensions, bridge, procedures, views, and validation scripts

---

## How to Run (Suggested Demo Flow)

### Step 1 — Run ETL scripts
Run:
- `ETL_Most_Searched.py` → writes `fact_monthly_customer_behavior`
- `ETL_30Days.py` → writes `fact_monthly_customer_interaction`

### Step 2 — Run SQL modeling scripts
Execute in order:
1. `00_setup.sql`
2. `01_dimensions.sql`
3. `02_bridge_tables.sql`
4. `03_procedures.sql`
5. `04_views.sql`

### Step 3 — Validate and demo
- `91_validation_checks.sql` for data checks
- `90_analysis_examples.sql` for demo queries & example outputs

---

## Example Queries

Top 10 contracts by TV duration:
```sql
CALL workspace.csm_project.top10_contract_by_program('tv');
