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
RAW_DIR = PROJECT_ROOT / "data" / "raw"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"

# File name patterns we expect in data/raw
ACQUISITION_PREFIX = "acquisition"
PERFORMANCE_PREFIX = "performance"


def find_files(prefix: str):
    """
    Find all CSV files in data/raw whose names start with the given prefix.
    Example: acquisition_2000Q1.csv, performance_2000Q1.csv
    """
    pattern = f"{prefix}_*.csv"
    files = sorted(RAW_DIR.glob(pattern))
    logging.info(f"Found {len(files)} file(s) for pattern: {pattern}")
    return files


def ingest_file(path: Path, kind: str) -> int:
    """
    Read a single CSV file into a DataFrame, normalize column names,
    and write it out as a Parquet file into data/staging.
    Returns number of rows.
    """
    logging.info(f"Reading {kind} file: {path.name}")

    # You can tweak read_csv options later (chunksize, dtypes, etc.)
    df = pd.read_csv(path, sep="|", header=None, engine="python")
    
    # # Normalize column names: strip spaces, lower-case
    # df.columns = [c.strip().lower() for c in df.columns]
    
     # Assign clean generic column names: col_0, col_1, col_2, ...
    df.columns = [f"col_{i}" for i in range(len(df.columns))]

    row_count = len(df)
    col_count = len(df.columns)
    logging.info(f"{kind} file '{path.name}' -> {row_count:,} rows, {col_count} columns")

    # Ensure staging dir exists
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # Write to Parquet with same base name
    out_path = STAGING_DIR / f"{path.stem}.parquet"
    df.to_parquet(out_path, index=False)
    logging.info(f"Wrote staging file: {out_path.relative_to(PROJECT_ROOT)}")

    return row_count


def run_ingestion():
    """
    Ingest all acquisition and performance files from data/raw
    into data/staging as Parquet.
    """
    if not RAW_DIR.exists():
        logging.error(f"Raw directory does not exist: {RAW_DIR}")
        return

    total_rows = 0

    for kind, prefix in (("acquisition", ACQUISITION_PREFIX),
                         ("performance", PERFORMANCE_PREFIX)):
        files = find_files(prefix)
        if not files:
            logging.warning(
                f"No {kind} files found in {RAW_DIR} "
                f"with pattern '{prefix}_*.csv'"
            )
            continue

        for path in files:
            total_rows += ingest_file(path, kind)

    logging.info(f"Finished ingestion. Total rows processed: {total_rows:,}")


if __name__ == "__main__":
    run_ingestion()
