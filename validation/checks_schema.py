from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

EXPECTED_NUM_COLUMNS = 108  # for the parquet files (before DuckDB adds filename)

# --- Rule thresholds ---
MAX_NULL_PCT_LOAN_ID = 0.0      # fail if any null loan_id
MAX_NULL_PCT_RPT_PD = 0.0       # fail if any null monthly reporting period

def _pct(n: int, d: int) -> float:
    return 0.0 if d == 0 else (100.0 * n / d)

def validate_parquet_schema(pat: Path) -> bool:
    logging.info(f"Validating schema for: {pat.name}")

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logging.error(f"Failed to read Parquet file {path.name}: {e}")
        return False

    # Check 0: not empty
    if df.empty:
        logging.error(f"{path.name}: DataFrame is empty.")
        return False

    # Check 1: column count
    actual_cols = len(df.columns)
    if actual_cols != EXPECTED_NUM_COLUMNS:
        logging.error(f"{path.name}: Expected {EXPECTED_NUM_COLUMNS} columns, found {actual_cols}.")
        return False

    # Check 2: required columns exist (now that you have named parquet)
    required = ["loan_id", "mnthly_rpt_pd"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logging.error(f"{path.name}: Missing required columns: {missing}")
        return False

    rows = len(df)

    # Check 3: loan_id not null
    loan_id_nulls = int(df["loan_id"].isna().sum())
    if loan_id_nulls > 0:
        logging.error(
            f"{path.name}: loan_id has {loan_id_nulls:,} nulls "
            f"({_pct(loan_id_nulls, rows):.2f}%)."
        )
        return False

    # Check 4: reporting period not null + shape check
    rpt_nulls = int(df["mnthly_rpt_pd"].isna().sum())
    if rpt_nulls > 0:
        logging.error(
            f"{path.name}: mnthly_rpt_pd has {rpt_nulls:,} nulls "
            f"({_pct(rpt_nulls, rows):.2f}%)."
        )
        return False

    # Shape check: allow 5 or 6 digits (your sample includes both like 12010 and 102009)
    rpt_as_str = df["mnthly_rpt_pd"].astype("Int64").astype(str)
    bad_len = ~rpt_as_str.str.len().isin([5, 6])
    bad_len_count = int(bad_len.sum())
    if bad_len_count > 0:
        logging.warning(
            f"{path.name}: mnthly_rpt_pd has {bad_len_count:,} values not length 5 or 6 "
            f"(showing up to 5): {rpt_as_str[bad_len].head(5).tolist()}"
        )

    # Check 5 (warning): unused_field should be empty
    if "unused_field" in df.columns:
        unused_non_null = int(df["unused_field"].notna().sum())
        if unused_non_null > 0:
            logging.warning(f"{path.name}: unused_field has {unused_non_null:,} non-null values (expected all null).")
        else:
            logging.info(f"{path.name}: unused_field is all null (as expected).")

    logging.info(f"{path.name}: schema+DQ OK ({actual_cols} columns, {rows:,} rows).")
    return True


def run_schema_checks() -> bool:
    if not STAGING_DIR.exists():
        logging.error(f"Staging directory does not exist: {STAGING_DIR}")
        return False

    # IMPORTANT: validate the NAMED parquet as the new contract
    parquet_files = sorted(STAGING_DIR.glob("*_named.parquet"))
    if not parquet_files:
        logging.warning(f"No *_named.parquet files found in {STAGING_DIR}")
        return False

    all_ok = True
    for path in parquet_files:
        ok = validate_parquet_schema(path)
        if not ok:
            all_ok = False

    if all_ok:
        logging.info("✅ All schema checks passed.")
    else:
        logging.warning("❌ One or more schema checks failed.")

    return all_ok


if __name__ == "__main__":
    run_schema_checks()
