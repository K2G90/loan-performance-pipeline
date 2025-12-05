-- List all tables/views
SHOW TABLES;

-- Show columns
Describe raw.performance_all;

-- Generic row count
SELECT COUNT(*) AS row_count FROM raw.performance_all;

-- Peek at data
SELECT * FROM raw.performance_all LIMIT 10;