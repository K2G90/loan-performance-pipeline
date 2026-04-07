SHOW ALL TABLES;

SELECT * FROM modeled.dim_time ORDER BY reporting_period_key LIMIT 15;

SELECT * FROM modeled.dim_loan LIMIT 10;

SELECT * FROM modeled.fact_loan_performance LIMIT 10;

SELECT * FROM analytics.monthly_metrics LIMIT 25;
