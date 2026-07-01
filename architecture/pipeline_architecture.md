# Pipeline Architecture

This document explains the current local architecture for the Loan Performance Data Pipeline.

The pipeline is designed to turn raw Fannie Mae loan performance files into validated, modeled, analysis-ready data using Python, Parquet, DuckDB, and SQL.

## Architecture Flow

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

Layer Breakdown
1. Dataset Glossary

The official Fannie Mae dataset glossary is treated as the source of truth for schema metadata.

Instead of manually guessing column names, the pipeline uses glossary metadata to generate config files that define how raw positional columns should be named.

2. Schema Config Generation

Script:

scripts/build_config_from_glossary.py

This script reads the glossary and generates schema config files.

Current config outputs:

config/columns_performance.csv
config/columns_acquisition.csv

These configs help keep schema naming consistent and auditable.

3. Raw Data Zone

Raw files are stored locally under:

data/raw/

These files are not committed to Git.

The raw files represent source input and should remain unchanged so the pipeline can be rerun from the same starting point.

4. Ingestion Layer

Script:

ingestion/ingest_loans.py

The ingestion layer reads raw pipe-delimited files and writes them to Parquet.

This step focuses on converting raw source files into a more efficient storage format without applying business modeling logic.

5. Staging Layer

Script:

staging/standardize_columns.py

The staging layer applies the generated config mappings to rename raw positional columns into meaningful glossary-aligned names.

Example standardized fields:

loan_id
monthly_reporting_period
current_interest_rate
original_unpaid_principal_balance
current_actual_unpaid_principal_balance
6. Validation Layer

Script:

validation/checks_schema.py

Validation acts as a quality gate before warehouse loading and modeling.

Current checks include:

File readability
Non-empty datasets
Expected column count
Required field presence
Basic key-field quality checks

Future validation can expand into duplicate checks, range checks, referential integrity checks, and data drift checks.

7. DuckDB Warehouse

Scripts:

warehouse/setup_warehouse.py
warehouse/run_sql.py

DuckDB is used as the local analytical warehouse.

Current raw warehouse views include:

raw.performance_all
raw.acquisition_all

These views expose standardized Parquet data through SQL.

8. Modeled Layer

SQL file:

modeling/star_schema.sql

The modeled layer creates business-friendly semantic views on top of the raw warehouse views.

Current modeled views:

modeled.dim_time
modeled.dim_loan
modeled.fact_loan_performance

The fact view represents monthly loan performance activity, while dimensions provide descriptive context.

9. Analytics Layer

Current analytics view:

analytics.monthly_metrics

This layer calculates business-facing metrics such as:

Active loans
Fact row count
Total current actual unpaid principal balance
Average current interest rate
Design Principles

This architecture follows a few core principles:

Keep raw data separate from transformed data
Use metadata-driven schema mapping instead of hardcoded assumptions
Validate data before warehouse modeling
Separate ingestion, staging, validation, modeling, and analytics responsibilities
Keep generated data artifacts out of version control
Design the local pipeline in a way that can later map to cloud services
Future Cloud Architecture

The current pipeline is local-first, but the design can be extended to a cloud architecture.

A future AWS version could look like:

S3 Raw Zone
        ↓
AWS Glue / Spark
        ↓
S3 Curated Zone
        ↓
Athena or Redshift
        ↓
dbt-style Modeling
        ↓
Analytics / Dashboard

Potential future services:

Amazon S3 for raw and curated storage
AWS Glue for cataloging and distributed transformation
AWS Lambda or Step Functions for orchestration
Amazon Redshift or Athena for analytics querying
GitHub Actions for CI/CD
CloudWatch for logging and monitoring
Current Status

The current local architecture supports:

Glossary-driven config generation
Raw CSV ingestion
Parquet staging
Column standardization
Schema/data-quality validation
DuckDB warehouse views
Modeled views
Monthly analytics metrics

The next major architecture improvements are stronger validation rules, improved dimensional modeling, automated tests, and CI/CD.
