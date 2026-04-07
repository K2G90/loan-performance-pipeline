--modeling/star_schema.sql
CREATE SCHEMA IF NOT EXISTS modeled;
CREATE SCHEMA IF NOT EXISTS analytics;

--Cleanup
DROP VIEW IF EXISTS modeled.fat_loan_performance;
-- -------------------------
-- dim_time
-- -------------------------
CREATE OR REPLACE VIEW modeled.dim_time AS
WITH base AS (
    SELECT DISTINCT mnthly_rpt_pd
    FROM raw.performance_all
    WHERE mnthly_rpt_pd IS NOT NULL
),
normalized AS (
    SELECT
        mnthly_rpt_pd AS reporting_period_key,
        LPAD(CAST(mnthly_rpt_pd AS VARCHAR), 6, '0') AS mmYYYY_str
    FROM base
)
SELECT
    reporting_period_key,
    mmYYYY_str,
    CAST(SUBSTR(mmYYYY_str, 1, 2) AS INTEGER) AS month,
    CAST(SUBSTR(mmYYYY_str, 3, 4) AS INTEGER) AS year
FROM normalized;

-- -------------------------
-- dim_loan (thin for now)
-- -------------------------
CREATE OR REPLACE VIEW modeled.dim_loan AS
SELECT DISTINCT
    loan_id,
    loan_purpose,
    property_type
FROM raw.performance_all
WHERE loan_id IS NOT NULL;

-- -------------------------
-- fact_loan_performance (monthly grain)
-- -------------------------
CREATE OR REPLACE VIEW modeled.fact_loan_performance AS
SELECT
  loan_id,
  mnthly_rpt_pd AS reporting_period_key,
  -- Temporary correction based on observed values:
  interest_rate AS unpaid_principal_bal,
  unpaid_principal_bal AS interest_rate,
  filename
FROM raw.performance_all
WHERE loan_id IS NOT NULL
  AND mnthly_rpt_pd IS NOT NULL;

-- -------------------------
-- first analytics view off the model
-- -------------------------
CREATE OR REPLACE VIEW analytics.monthly_metrics AS
SELECT
    t.year,
    t.month,
    f.reporting_period_key,
    COUNT(DISTINCT f.loan_id) AS active_loans,
    COUNT(*) AS fact_rows,
    SUM(f.unpaid_principal_bal) AS total_upb,
    AVG(f.interest_rate) AS avg_interest_rate
    FROM modeled.fact_loan_performance f
    JOIN modeled.dim_time t
        ON t.reporting_period_key = f.reporting_period_key
    GROUP BY 1,2,3
    ORDER BY 1,2,3;
