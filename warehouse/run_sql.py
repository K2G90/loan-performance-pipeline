from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"
SQL_FILE = PROJECT_ROOT / "analytics" / "sample_checks.sql"

con = duckdb.connect(DB_PATH.as_posix())
with open(SQL_FILE) as f:
    sql = f.read()
for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
    print("\n-- Executing --")
    print(stmt[:120] + ("..." if len(stmt) > 120 else ""))
    try:
        print(con.execute(stmt).fetchdf())
    except Exception as e:
        print(f"(Skipped / Error) {e}")
con.close()
