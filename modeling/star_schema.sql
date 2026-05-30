-- modeling/star_schema.sql
CREATE SCHEMA IF NOT EXISTS modeled;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Cleanup old typo from earlier development
DROP VIEW IF EXISTS modeled.fat_loan_performance;

-- -------------------------
-- dim_time
-- -------------------------
CREATE OR REPLACE VIEW modeled.dim_time AS
WITH base AS (
    SELECT DISTINCT monthly_reporting_period
    FROM raw.performance_all
    WHERE monthly_reporting_period IS NOT NULL
),
normalized AS (
    SELECT
        monthly_reporting_period AS reporting_period_key,
        LPAD(CAST(monthly_reporting_period AS VARCHAR), 6, '0') AS mmYYYY_str
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
    channel,
    seller_name,
    servicer_name,
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
    monthly_reporting_period AS reporting_period_key,
    current_actual_unpaid_principal_balance,
    current_interest_rate,
    original_unpaid_principal_balance,
    original_interest_rate,
    filename
FROM raw.performance_all
WHERE loan_id IS NOT NULL
  AND monthly_reporting_period IS NOT NULL;

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
    SUM(f.current_actual_unpaid_principal_balance) AS total_current_actual_upb,
    AVG(f.current_interest_rate) AS avg_current_interest_rate
FROM modeled.fact_loan_performance f
JOIN modeled.dim_time t
    ON t.reporting_period_key = f.reporting_period_key
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
