from pathlib import Path
import sys
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"

# Default stays the same as current behavior
DEFAULT_SQL_FILE = PROJECT_ROOT / "analytics" / "sample_checks.sql"


def resolve_sql_file() -> Path:
    """
    Allow: python warehouse/run_sql.py modeling/star_schema.sql
    If no arg is passed, fall back to analytics/sample_checks.sql.
    """
    if len(sys.argv) >= 2:
        candidate = PROJECT_ROOT / sys.argv[1]
        return candidate
    return DEFAULT_SQL_FILE


def main() -> None:
    sql_file = resolve_sql_file()
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    con = duckdb.connect(DB_PATH.as_posix())

    sql = sql_file.read_text()

    # Split on semicolons. This is fine for your current SQL style.
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for stmt in statements:
        print("\n-- Executing --")
        print(stmt[:120] + ("..." if len(stmt) > 120 else ""))
        try:
            # fetchdf() works great for SELECT/SHOW/DESCRIBE
            print(con.execute(stmt).fetchdf())
        except Exception as e:
            # For DDL (CREATE VIEW, CREATE SCHEMA, etc.) fetchdf() may not apply
            # We still want to run it and keep going.
            try:
                con.execute(stmt)
                print("(OK)")
            except Exception as e2:
                print(f"(Skipped / Error) {e2}")

    con.close()


if __name__ == "__main__":
    main()
