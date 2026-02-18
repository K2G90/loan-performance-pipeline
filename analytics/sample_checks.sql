-- List all tables/views
SHOW ALL TABLES;

-- Show columns
DESCRIBE raw.performance_all;

-- Generic row count
SELECT COUNT(*) AS row_count FROM raw.performance_all;

-- Peek at data
SELECT * FROM raw.performance_all LIMIT 10;


SELECT table_schema, table_name
FROM information_schema.tables
ORDER BY 1, 2;
