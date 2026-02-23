--modelinig/star_schemal.sql
CREATE SCHEMA IF NOT EXISTS modeled;
CREATE SCHEMA IF NOT EXISTS analytics;

-- -------------------------
-- dim_time
-- -------------------------
CREATE OR REPLACE VIEW modeled.dim_time AS
WITH base AS (
    SELECT DISTINCT mnthly_rpt_pd
    FROM raw.performance_all
    WHERE monthly_rpt_pd IS NOT NULL
),
parsed AS (
    SELECT
    mnthly_rpt_pd AS reporting_period_key,
    CAST(mnthly_rpt_pd AS VARCHAR) AS rp_str
    FROM base
)
SELECT
    reporting_period_key,
    rp_str,
    CASE
        WHEN LENGTH(rp_str) = 5 THEN CAST(SUBSTR(rp_str, 1, 1) AS INTEGER)
        WHEN LENGTH(rp_str) = 6 THEN CAST(SUBSTR(rp_str, 1, 2) AS INTEGER)
        ELSE NULL
    END AS month,
    CASE
        WHEN LENGTH(rp_str) = 5 THEN CAST('20' || SUBSTR(rp_str, 2, 4) AS INTEGER)
        WHEN LENGTH(rp_str) = 6 THEN CAST(SUBSTR(rp_str, 3, 4) AS INTEGER)
        ELSE NULL
    END AS year
    FROM parsed;

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
CREATE OR REPLACE VIEW modeled.fat_loan_performance AS
SELECT
    loan_id,
    mnthly_rpt_pd AS reporting_period_key,
    unpaid_principal_bal,
    interest_rate,
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
    f.reporting_period_key
    COUNT(DISTINCT f.loan_id) AS active_loans,
    COUNT(*) AS fact_rows,
    SUM(f.unpaid_principal_bal) AS total_upb,
    AVG(f.interest_rate) AS avg_interest_rate
    FROM modeled.fact_loan_performance f
    JOIN modeled.dim_time t
        ON t.reporting_period_key = f.reporting_period_key
    GROUP BY 1,2,3
    ORDER BY 1,2,3;
