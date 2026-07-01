# Loan Performance Data Pipeline

_End-to-end data engineering pipeline for Fannie Mae loan performance data._

## Project Overview

This project is a production-minded data engineering pipeline that processes public Fannie Mae loan performance data through ingestion, staging, schema standardization, validation, warehouse modeling, and analytics.

The project started as a local pipeline using Python, Parquet, DuckDB, and SQL. It has since evolved into a metadata-driven pipeline that uses the official Fannie Mae dataset glossary to generate schema configuration files instead of relying on manually guessed column names.

The goal of this project is to demonstrate practical data engineering skills including:

- ETL pipeline design
- Metadata-driven schema mapping
- Data validation
- Warehouse modeling
- SQL-based analytics
- Secure data handling
- Production-minded repo structure

---

## Why This Project Exists

Fannie Mae loan performance files are public, but they are not immediately analysis-ready.

The files are pipe-delimited and may not include headers. This means the pipeline needs a reliable way to map raw positional columns to meaningful business names.

Instead of hardcoding column names directly into transformation logic, this project uses the official Fannie Mae glossary as the source of truth for schema metadata.

That glossary-driven approach helps make the pipeline more:

- Reproducible
- Maintainable
- Easier to audit
- Less dependent on manual naming assumptions
- Better aligned with real-world data governance practices

---

## Tech Stack

- **Python** — ingestion, staging, validation, orchestration scripts
- **Pandas** — data loading and transformation
- **PyArrow / Parquet** — efficient columnar storage
- **DuckDB** — local analytical warehouse
- **SQL** — warehouse views, modeled layer, analytics
- **Fannie Mae Dataset Glossary** — source metadata for schema mapping
- **Git / GitHub** — version control and portfolio presentation
- **Pre-commit hooks** — repository guardrails and code hygiene

---

## Architecture Overview
For a more detailed explanation of each pipeline layer, see [Pipeline Architecture](architecture/pipeline_architecture.md).

Current local architecture:

```text
Fannie Mae Dataset Glossary
        ↓
Schema Config Generator
        ↓
Generated Config Files
        ↓
Raw Pipe-Delimited CSV Files
        ↓
Ingestion Layer
        ↓
Staging Parquet Files
        ↓
Column Standardization
        ↓
Named Parquet Files
        ↓
Schema + Data Quality Validation
        ↓
DuckDB Warehouse
        ↓
Modeled Layer
        ↓
Analytics Layer
```

The current pipeline is local-first, but it is designed with future cloud migration in mind.

A future cloud version could evolve toward:

```text
S3 → Glue / Spark → Redshift or Athena → dbt-style modeling → Analytics / Dashboard
```

---

## Current Pipeline Flow

### 1. Glossary-Driven Config Generation

Script:

```text
scripts/build_config_from_glossary.py
```

This script reads the official Fannie Mae glossary and generates config files used by the staging layer.

Generated config files:

```text
config/columns_performance.csv
config/columns_acquisition.csv
```

The config files include:

- Field position
- Column index
- Source field name
- Physical name
- Logical name
- Source data type
- Source max length
- Field description
- Schema column count

This allows the project to move from inferred column names to metadata-driven schema mapping.

---

### 2. Ingestion Layer

Script:

```text
ingestion/ingest_loans.py
```

The ingestion layer reads raw pipe-delimited CSV files and writes them as Parquet files into the staging area.

Input:

```text
data/raw/
```

Output:

```text
data/staging/
```

The ingestion layer focuses on moving raw files into a more efficient and consistent format. It does not apply business modeling logic.

---

### 3. Staging / Column Standardization

Script:

```text
staging/standardize_columns.py
```

The staging layer applies glossary-generated config mappings to the staged Parquet files.

It converts unnamed positional columns into meaningful logical names such as:

```text
loan_id
monthly_reporting_period
current_interest_rate
original_unpaid_principal_balance
current_actual_unpaid_principal_balance
```

Output examples:

```text
data/staging/performance_sample_named.parquet
data/staging/acquisition_sample_named.parquet
```

These files are generated locally and are not committed to version control.

---

### 4. Validation Layer

Script:

```text
validation/checks_schema.py
```

Validation acts as a quality gate before data reaches the warehouse and modeled layers.

Current validation checks include:

- File readability
- Non-empty datasets
- Expected column count
- Required field presence
- Basic null checks on key fields
- Schema/data-quality pass or fail logging

Current successful validation example:

```text
schema+DQ OK (108 columns, 757 rows)
All schema checks passed
```

Future validation enhancements may include:

- Duplicate loan/month detection
- Value range checks
- Referential integrity checks
- Data drift checks
- Richer data quality reporting

---

### 5. DuckDB Warehouse

Scripts:

```text
warehouse/setup_warehouse.py
warehouse/run_sql.py
```

DuckDB is used as a local analytical warehouse.

Warehouse views include:

```text
raw.performance_all
raw.acquisition_all
```

These views read from named Parquet files and expose the standardized schema for downstream modeling.

---

### 6. Modeled Layer

SQL file:

```text
modeling/star_schema.sql
```

The modeled layer creates semantic views on top of the raw warehouse views.

Current modeled views:

```text
modeled.dim_time
modeled.dim_loan
modeled.fact_loan_performance
```

The modeled layer uses glossary-aligned field names such as:

```text
monthly_reporting_period
current_actual_unpaid_principal_balance
current_interest_rate
original_unpaid_principal_balance
original_interest_rate
```

This layer separates raw data structure from business-facing analytics.

---

### 7. Analytics Layer

Current analytics view:

```text
analytics.monthly_metrics
```

This view currently calculates monthly metrics such as:

- Active loans
- Fact row count
- Total current actual unpaid principal balance
- Average current interest rate

This layer is intentionally simple for now and will expand as the modeled layer becomes richer.

---

## Folder Structure

```text
analytics/       SQL checks and analytics views
architecture/    Architecture notes and diagrams
config/          Glossary-generated schema config files
data/            Local raw/staging/curated data zones
ingestion/       Raw file ingestion scripts
modeling/        Star schema and modeled SQL views
notebooks/       Optional exploratory analysis area
orchestrator/    Pipeline orchestration scripts
scripts/         Utility scripts, including config generation
staging/         Column standardization logic
validation/      Schema and data-quality checks
warehouse/       DuckDB setup and SQL runner scripts
```

---

## How to Run Locally

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate schema configs from the glossary

Example:

```bash
python scripts/build_config_from_glossary.py \
  --glossary data/reference/crt-file-layout-and-glossary.xlsx \
  --data-file data/raw/performance_sample.csv \
  --output config/columns_performance.csv
```

Repeat for acquisition if needed:

```bash
python scripts/build_config_from_glossary.py \
  --glossary data/reference/crt-file-layout-and-glossary.xlsx \
  --data-file data/raw/acquisition_sample.csv \
  --output config/columns_acquisition.csv
```

### 4. Run ingestion

```bash
python ingestion/ingest_loans.py
```

### 5. Standardize staged columns

```bash
python staging/standardize_columns.py
```

### 6. Run validation

```bash
python validation/checks_schema.py
```

### 7. Build the DuckDB warehouse

```bash
python warehouse/setup_warehouse.py
```

### 8. Build modeled and analytics views

```bash
python warehouse/run_sql.py modeling/star_schema.sql
```

### 9. Check model output

```bash
python warehouse/run_sql.py analytics/check_model.sql
```

---

## Data and Security Handling

This project uses publicly available Fannie Mae loan performance data.

Even though the data is public, the project treats data artifacts carefully by default.

The repository is designed as a code-first project:

- Raw data files are not committed
- Staged Parquet files are not committed
- Curated data artifacts are not committed
- DuckDB warehouse files are not committed
- Environment files are ignored
- Generated data artifacts can be rebuilt locally from source files and scripts

Ignored examples:

```text
data/**
*.parquet
*.zip
warehouse/*.duckdb
.env
.env.*
```

Committed examples:

```text
source code
SQL models
schema config files
documentation
README
architecture notes
```

In a future cloud deployment, this project would use:

- Encrypted object storage
- Least-privilege IAM roles
- Environment separation
- Secure secrets management
- CI/CD guardrails

---

## Validation Philosophy

Validation is treated as a pipeline quality gate.

The goal is to catch schema and data-quality issues before data reaches the warehouse, modeled layer, or analytics layer.

Current validation focuses on:

- Does the file exist and load?
- Does the dataset have rows?
- Does the column count match expectations?
- Do required key fields exist?
- Are key fields populated enough to support modeling?

Future validation will expand into:

- Duplicate checks
- Range checks
- Field-level business rules
- Cross-table consistency checks
- Data quality reports

---

## Current Status

Completed:

- Local project structure
- Raw CSV ingestion
- CSV to Parquet conversion
- Glossary-driven config generation
- Metadata-based column standardization
- Schema/data-quality validation
- DuckDB warehouse setup
- Modeled layer with `dim_time`, `dim_loan`, and `fact_loan_performance`
- Monthly analytics view with UPB and interest rate metrics
- Git guardrails for data/security handling

In progress:

- README and GitHub presentation upgrade
- Architecture documentation
- Stronger validation rules
- Improved modeled layer design

---

## Roadmap

Near-term:

- Add architecture documentation and diagrams
- Improve README setup instructions
- Add richer validation checks
- Improve `dim_loan` to reduce duplicate loan attribute rows
- Add more analytics metrics

Mid-term:

- Add GitHub Actions CI for automated checks
- Add tests for config generation and validation
- Add data profiling outputs
- Add more robust error handling and logging
- Add generated reports for pipeline runs

Future:

- Add dbt-style transformations or dbt integration
- Add Airflow-style orchestration
- Add AWS cloud version using S3, Glue, and Redshift/Athena
- Add dashboard or API layer
- Add observability and monitoring concepts

---

## Author

Cedric Williams
Data Engineer / Backend Engineer
Python | SQL | DuckDB | ETL Pipelines | Data Modeling | AWS
