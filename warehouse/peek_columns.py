from pathlib import Path
import duckdb

# PROJECT_ROOT = Path(__file__).resolve().parents[1]
# DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"

# con = duckdb.connect(DB_PATH.as_posix())

# print("\nTables/Views:")
# print(con.execute("SHOW TABLES;").fetchdf())

# print("\nColumns in raw.performance_all:")
# print(con.execute("DESCRIBE raw.performance_all;").fetchdf().to_string(index=False))

# print("\nSample rows:")
# print(con.execute("SELECT * FROM raw.performance_all LIMIT 5;").fetchdf())

# con.close()


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "warehouse" / "loans_dw.duckdb"

con = duckdb.connect(DB_PATH.as_posix())

print("\nDB:", DB_PATH)

# 1) Show tables/views across ALL schemas (more reliable than SHOW TABLES)
print("\nAll tables/views (information_schema):")
print(
    con.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        ORDER BY 1, 2;
    """).fetchdf().to_string(index=False)
)

# 2) Confirm the raw views exist and have rows
print("\nRow counts:")
queries = [
    ("raw.performance_all", "SELECT COUNT(*) AS rows FROM raw.performance_all;"),
    ("raw.acquisition_all", "SELECT COUNT(*) AS rows FROM raw.acquisition_all;"),
]
for name, q in queries:
    try:
        df = con.execute(q).fetchdf()
        print(f"{name}:")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"{name}: (not available) -> {e}")

# 3) Show column layout (schema)
print("\nColumns in raw.performance_all:")
print(con.execute("DESCRIBE raw.performance_all;").fetchdf().to_string(index=False))

# 4) Show sample rows
print("\nSample rows (raw.performance_all):")
print(con.execute("SELECT * FROM raw.performance_all LIMIT 5;").fetchdf())

# 5) Actual null profiling for a few columns (data content, not schema constraint)
print("\nNull profile (first 12 columns):")
try:
    cols = con.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='raw' AND table_name='performance_all'
        ORDER BY ordinal_position
        LIMIT 12;
    """).fetchall()
    cols = [c[0] for c in cols]

    # Build a query like: count nulls for each col
    parts = ["COUNT(*) AS total_rows"]
    for c in cols:
        parts.append(f"COUNT(*) FILTER (WHERE {c} IS NULL) AS {c}_nulls")
    sql = "SELECT " + ", ".join(parts) + " FROM raw.performance_all;"
    print(con.execute(sql).fetchdf().to_string(index=False))
except Exception as e:
    print("Null profile skipped:", e)

con.close()
