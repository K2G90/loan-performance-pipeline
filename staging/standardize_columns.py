from pathlib import Path
import pandas as pd
import logging

# If data/staging is empty → run orchestrator/run_pipeline.py first

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
CONFIG_DIR = PROJECT_ROOT / "config"

def load_column_map(kind: str) -> list[str]:
    path = CONFIG_DIR / f"columns_{kind}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Column map file not found: {path}")
    cols = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    # print(cols)
    return cols


def infer_kind(filename: str) -> str:
    name = filename.lower()
    if name.startswith("performance_"):
        return "performance"
    if name.startswith("acquisition_"):
        return "acquisition"
    return "unknown"


def standardize_one(path: Path) -> Path:
    kind = infer_kind(path.name)
    if kind == "unknown":
        logging.info(f"Skipping unknown file type: {path.name}")
        return path


    df = pd.read_parquet(path)
    mapping = load_column_map(kind)


    if len(mapping) != len(df.columns):
        raise ValueError(
            f"{path.name}: column map has {len(mapping)} names, parquet has {len(df.columns)} columns."
        )

    df.columns = mapping

    out = path.with_name(path.stem + "_named.parquet")
    df.to_parquet(out, index=False)
    logging.info(f"Wrote: {out.relative_to(PROJECT_ROOT)} ({len(df):,} rows, {len(df.columns)} cols)")
    return out

def main():
    if not STAGING_DIR.exists():
        raise FileNotFoundError(f"Staging dir not found: {STAGING_DIR}")

    parquet_files = sorted(STAGING_DIR.glob("*.parquet"))
    if not parquet_files:
        logging.warning(f"No parquet files found in {STAGING_DIR}")
        return

    for p in parquet_files:
        # Avoid re-processing already named files
        if p.name.endswith("_named.parquet"):
            continue
        standardize_one(p)

if __name__ == "__main__":
    main()
