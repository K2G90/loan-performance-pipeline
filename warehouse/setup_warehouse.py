from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

con = duckdb.connect(DB_PATH.as_posix())

# Create schemas
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
con.execute("CREATE SCHEMA IF NOT EXISTS curated;")

# Register all parquet files in staging as external tables (read-only views)
# Start with performance_* & acquisition_* if they exist.

perf_glob= (STAGING_DIR / "performance_*.parquet").as_posix()
acq_glob = (STAGING_DIR / "acquisition_*.parquet").as_posix()

#Create or replace raw tables from parquet (external scans).
con.execute(f""" CREATE OR REPLACE VIEW raw.performance_all AS 
            SELECT * FROM read_parquet('{perf_glob}', filename=True);
            """)

con.execute(f""" CREATE OR REPLACE VIEW raw.acquisition_all AS 
            SELECT * FROM read_parquet('{acq_glob}', filename=True);
            """)

#Checks
perf_count = con.execute("SELECT COUNT(*) FROM raw.performance_all").fetchone()[0]
print(f"Performance rows: {perf_count:,}")

#See if acquisition table exists before querying
try:
    acq_count = con.execute("SELECT COUNT(*) FROM raw.acquisition_all").fetchone()[0]
    print(f"Acquisition rows: {acq_count:,}")
except Exception as e:
    print("Acquisition table does not exist yet.")

con.execute(f""" CREATE OR REPLACE VIEW curated.fact_loan_performannce AS
            SELECT * FROM raw.performance_all""")

print(f"DuckDB warehouse setup complete. DB path: {DB_PATH}")
con.close()