from pathlib import Path
import logging
import pandas as pd

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

# For the modern single-file Fannie Mae layout
EXPECTED_NUM_COLUMNS = 108  # can tweak once you confirm from docs / exploration


def validate_parquet_schema(path: Path) -> bool:
    """
    Basic schema validation for a single Parquet file:
    - file can be read
    - not empty
    - expected number of columns
    Returns True if all checks pass, False otherwise.
    """
    logging.info(f"Validating schema for: {path.name}")

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logging.error(f"Failed to read Parquet file {path.name}: {e}")
        return False

    # Check 1: not empty
    if df.empty:
        logging.error(f"{path.name}: DataFrame is empty.")
        return False

    # Check 2: column count
    actual_cols = len(df.columns)
    if actual_cols != EXPECTED_NUM_COLUMNS:
        logging.error(
            f"{path.name}: Expected {EXPECTED_NUM_COLUMNS} columns, "
            f"found {actual_cols}."
        )
        return False

    logging.info(
        f"{path.name}: schema OK ({actual_cols} columns, {len(df):,} rows)."
    )
    return True


def run_schema_checks() -> bool:
    """
    Run schema validation on all Parquet files in data/staging.
    Returns True if all files pass, False otherwise.
    """
    if not STAGING_DIR.exists():
        logging.error(f"Staging directory does not exist: {STAGING_DIR}")
        return False

    parquet_files = sorted(STAGING_DIR.glob("*.parquet"))
    if not parquet_files:
        logging.warning(f"No Parquet files found in {STAGING_DIR}")
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
