# orchestrator/run_pipeline.py
from pathlib import Path
import importlib
import logging
import sys

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Resolve project root so imports work no matter where we run from
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

def main() -> int:
    logging.info("🚀 Starting pipeline: ingestion ➜ schema validation")

    # 1) Ingestion
    try:
        ingest = importlib.import_module("ingestion.ingest_loans")
        ingest.run_ingestion()
        logging.info("✅ Ingestion finished.")
    except Exception as e:
        logging.exception(f"❌ Ingestion failed: {e}")
        return 1

    # 2) Schema validation
    try:
        checks = importlib.import_module("validation.checks_schema")
        all_ok = checks.run_schema_checks()
        if not all_ok:
            logging.warning("❌ One or more schema checks failed.")
            return 2
        logging.info("✅ All schema checks passed.")
    except Exception as e:
        logging.exception(f"❌ Schema checks failed: {e}")
        return 3

    logging.info("🎉 Pipeline finished successfully.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
