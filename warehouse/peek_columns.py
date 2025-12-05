from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"

con = duckdb.connect(DB_PATH.as_posix())

print("\nTables/Views:")
print(con.execute("SHOW TABLES;").fetchdf())

print("\nColumns in raw.performance_all:")
print(con.execute("DESCRIBE raw.performance_all;").fetchdf().to_string(index=False))

print("\nSample rows:")
print(con.execute("SELECT * FROM raw.performance_all LIMIT 5;").fetchdf())

con.close()