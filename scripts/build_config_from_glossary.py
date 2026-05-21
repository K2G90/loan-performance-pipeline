"""
Build a schema/config mapping CSV from Fannie Mae's CRT File Layout and Glossary.

Why this exists:
- Fannie Mae loan performance files are pipe-delimited and usually have no header row.
- The glossary is the source-of-truth for field position, official field name, data type,
  max length, and descriptions.
- This script turns that glossary into a config file your pipeline can use to rename
  raw positional columns into stable internal names.

Example:
    python scripts/build_config_from_glossary.py \
      --glossary data/reference/crt-file-layout-and-glossary.xlsx \
      --data-file data/raw/full/2025Q4.csv \
      --output config/columns_performance.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_SHEET_NAME = "Combined Glossary"

# Optional logical-name overrides for fields where the official name is accurate,
# but the internal project name should be more canonical/readable.
# Keep this small and intentional. Everything else falls back to snake_case(Field Name).
LOGICAL_NAME_OVERRIDES: dict[str, str] = {
    "loan_identifier": "loan_id",
    "original_upb": "original_unpaid_principal_balance",
    "current_actual_upb": "current_actual_unpaid_principal_balance",
    "original_ltv": "original_loan_to_value_ratio",
    "original_cltv": "original_combined_loan_to_value_ratio",
    "debt_to_income": "debt_to_income_ratio",
    "monthly_reporting_period": "monthly_reporting_period",
}

SCHEMA_VARIANT_EXCLUSIONS: dict[int, set[str]] = {
    # 108-column sample layout excludes fields that exist in the full 113-column glossary.
    108: {
        "reference_pool_id",
        "master_servicer",
        "upb_at_issuance",
        "loan_age",
        "remaining_months_to_maturity",
    },

    # Full 113-column layout keeps all glossary fields.
    113: set(),
}


REQUIRED_GLOSSARY_COLUMNS = [
    "Field Position",
    "Field Name",
    "Description",
    "Single-Family (SF) Loan Performance",
    "Type",
    "Max Length",
]


def snake_case(value: Any) -> str:
    """Convert Fannie Mae field names into stable snake_case names."""
    if pd.isna(value):
        return "unknown"

    text = str(value).strip().lower()

    # Normalize common symbols/phrases before stripping punctuation.
    replacements = {
        "%": " percent ",
        "&": " and ",
        "/": " ",
        "-": " ",
        "\u2013": " ",
        "\u2014": " ",
        "(upb)": " upb ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")

    # Keep common abbreviations readable/consistent.
    text = text.replace("u_p_b", "upb")
    text = text.replace("l_t_v", "ltv")
    text = text.replace("c_l_t_v", "cltv")
    text = text.replace("d_t_i", "dti")

    return text or "unknown"


def detect_pipe_delimited_column_count(data_file: Path) -> int:
    """Detect column count from the first row of a pipe-delimited Fannie Mae file."""
    with data_file.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        first_row = next(reader)
    return len(first_row)


def load_glossary(glossary_path: Path, sheet_name: str) -> pd.DataFrame:
    """Load and validate the glossary layout sheet."""
    glossary = pd.read_excel(glossary_path, sheet_name=sheet_name)
    glossary.columns = [str(col).strip() for col in glossary.columns]

    missing = [col for col in REQUIRED_GLOSSARY_COLUMNS if col not in glossary.columns]
    if missing:
        raise ValueError(
            "Glossary is missing expected columns: "
            + ", ".join(missing)
            + f". Found columns: {list(glossary.columns)}"
        )

    glossary = glossary.dropna(subset=["Field Position", "Field Name"]).copy()
    glossary["Field Position"] = glossary["Field Position"].astype(int)
    glossary = glossary.sort_values("Field Position")

    return glossary


def build_config(glossary: pd.DataFrame, detected_column_count: int | None = None) -> pd.DataFrame:
    """Build pipeline config from glossary rows."""
    glossary = glossary.copy()

    if detected_column_count is not None:
        max_position = int(glossary["Field Position"].max())

        if detected_column_count > max_position:
            raise ValueError(
                f"Data file has {detected_column_count} columns, but glossary only has "
                f"{max_position} field positions. Get a newer glossary."
            )

        # If this detected width has an explicit schema variant rule,
        # do not truncate by field position here. We will remove known
        # omitted fields after normalized names are built.
        if detected_column_count not in SCHEMA_VARIANT_EXCLUSIONS:
            glossary = glossary[glossary["Field Position"] <= detected_column_count].copy()

    # Build normalized names on the glossary first so metadata stays attached
    # to the correct original glossary row.
    glossary["source_field_name"] = glossary["Field Name"].astype(str).str.strip()
    glossary["physical_name"] = glossary["source_field_name"].apply(snake_case)
    glossary["logical_name"] = glossary["physical_name"].apply(
        lambda name: LOGICAL_NAME_OVERRIDES.get(name, name)
    )

    exclusions = SCHEMA_VARIANT_EXCLUSIONS.get(detected_column_count, set())

    if exclusions:
        glossary = glossary[~glossary["logical_name"].isin(exclusions)].copy()

    glossary = glossary.reset_index(drop=True)

    if detected_column_count is not None and len(glossary) != detected_column_count:
        raise ValueError(
            f"Config row count ({len(glossary)}) does not match detected column count "
            f"({detected_column_count}). Check schema variant exclusions."
        )

    config = pd.DataFrame()

    # Rebuild positions after exclusions so the config matches the physical file layout.
    config["field_position"] = range(1, len(glossary) + 1)
    config["column_index"] = config["field_position"] - 1

    # Official Fannie Mae label and standardized project names.
    config["source_field_name"] = glossary["source_field_name"]
    config["physical_name"] = glossary["physical_name"]
    config["logical_name"] = glossary["logical_name"]

    config["source_data_type"] = glossary["Type"].astype(str).str.strip()
    config["source_max_length"] = glossary["Max Length"].astype(str).str.strip()
    config["sf_loan_performance"] = glossary[
        "Single-Family (SF) Loan Performance"
    ].astype(str).str.strip()
    config["description"] = (
        glossary["Description"]
        .astype(str)
        .str.replace("\n", " ", regex=False)
        .str.strip()
    )

    # Useful for schema-version checks downstream.
    config["schema_column_count"] = len(config)

    return config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Fannie Mae loan performance config from glossary."
    )
    parser.add_argument("--glossary", required=True, type=Path, help="Path to glossary XLSX")
    parser.add_argument("--output", required=True, type=Path, help="Output config CSV path")
    parser.add_argument(
        "--data-file",
        type=Path,
        default=None,
        help="Optional pipe-delimited data file used to detect expected column count",
    )
    parser.add_argument(
        "--sheet-name",
        default=DEFAULT_SHEET_NAME,
        help=f"Glossary sheet name. Default: {DEFAULT_SHEET_NAME}",
    )

    args = parser.parse_args()

    detected_column_count = None
    if args.data_file:
        detected_column_count = detect_pipe_delimited_column_count(args.data_file)
        print(f"Detected {detected_column_count} columns in {args.data_file}")

    glossary = load_glossary(args.glossary, args.sheet_name)
    config = build_config(glossary, detected_column_count)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    config.to_csv(args.output, index=False)

    print(f"Generated {len(config)} config rows")
    print(f"Wrote config to {args.output}")
    print("\nPreview:")
    print(config.head(12).to_string(index=False))


if __name__ == "__main__":
    main()
