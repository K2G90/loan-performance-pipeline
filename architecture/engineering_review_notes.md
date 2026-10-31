# Engineering Review Notes

## Purpose

Track observations, assumptions, proposed improvements, and follow-up work discovered while reviewing the Loan Performance Pipeline. These are **review notes**, not a claim that the improvements have been implemented.

**Review scope:** Codebase re-entry and LP-002 (Strengthen Loan Dimension Modeling).

## Confirmed code observations

| Component | Current behavior | Follow-up |
| --- | --- | --- |
| `staging/standardize_columns.py` | `load_column_map()` checks whether its config path exists before calling `pd.read_csv()`. | Consider targeted `try/except` around file reading, preserve explicit schema precondition checks, and log actionable errors. |
| `staging/standardize_columns.py` | Checks mapping length against the staged DataFrame's column count. | Count equality does not prove that a mapping matches the correct source layout; investigate schema-version verification. |
| `validation/checks_schema.py` | Expects exactly 108 columns. | Investigate deriving an expected width from a verified, approved schema configuration instead of a global constant. |
| `validation/checks_schema.py` | Defines `MAX_NULL_PCT_LOAN_ID` and `MAX_NULL_PCT_RPT_PD`, but directly checks null counts against zero. | Remove unused thresholds or intentionally implement threshold-based rules. |
| `validation/checks_schema.py` | Uses file-level success/failure booleans. The runner continues checking other discovered files after one failure. | Count total/passed/failed files; later consider structured per-file findings. |
| `validation/checks_schema.py` | Checks an optional `unused_field` for null values. | Reconcile this with the newer `reference_pool_id` field mapping and source layout. |
| `orchestrator/run_pipeline.py` | Runs ingestion followed by validation; standardization is not included. | Align orchestration with validation's expectation of `*_named.parquet` files. |
| `warehouse/run_sql.py` | Logs statement failures and continues rather than consistently failing the process. | Decide how SQL failures should propagate to callers and future CI jobs. |
| `.gitignore` | Includes global `*.csv` and `.github/` exclusions. | Verify intended version control of schema metadata CSVs and future GitHub Actions workflows. |
| `architecture/pipeline_architecture.md` | The architecture flow code fence appears to remain open in the committed document. | Repair Markdown rendering in a documentation-focused change. |
| `modeling/star_schema.sql` | `dim_loan` selects DISTINCT combinations of several attributes from performance data. | Measure loan ID uniqueness and attribute stability before deciding the dimension's intended grain. |

## Dependency and workspace housekeeping (deferred)

- **Observed:** On GitHub, `requirements.txt` and `requirements.lock` currently contain identical dependency listings. Revisit the intended role of each file, then separate direct project dependencies from any reproducible lock strategy. Do not change dependencies during the current code walkthrough.
- **User-confirmed:** Untracked `env` and `list` came from a separate Bash-versus-Zsh learning/cleanup exercise. Keep them out of LP-002 commits. Inspect and remove or relocate them only when ready; their contents have not been reviewed here.

## Proposed validation enhancements

- Report counts of files checked, files passed, and files failed, while retaining the current overall Boolean result for now.
- Distinguish file-level failure counts from individual rule violations within a file.
- Review fail-fast behavior versus collecting multiple failures per file.
- Validate approved schema variants using glossary metadata, not column count alone.
- Add model-integrity tests once the intended grains have been established.

- Consider semantic and temporal validation beyond required-column presence: confirm loan IDs and reporting periods are populated and well-formed, and investigate reporting-period relationships to origination/acquisition dates using the official source definitions. Do not assume an older loan's later monthly report is invalid; establish business rules first.

- **Proposed decision (pending source-spec verification):** Treat invalid `monthly_reporting_period` values as validation failures rather than warnings. Check the source's documented format, normalize legitimate five-digit `MMYYYY` values where leading zero is lost, and validate real calendar months (1–12), reasonable year bounds, and safe parsing before modeling. Report failures clearly and ensure orchestration blocks downstream modeling on failure. This remains **not implemented**.

- **Stale artifact risk (observed design gap, not yet reproduced):** `run_schema_checks()` scans all matching `*_named.parquet` files without proving they were produced by the current successful standardization run. Old named Parquet files could pass validation after a new run fails or skips standardization. Plan to connect the orchestrator stages, validate explicitly expected current-run outputs (for example via an output manifest/run ID), and stop downstream processing on failure. Distinguish a separately invoked validation command from automatic continuation by the orchestrator.

- **CLI exit-code gap:** `validation/checks_schema.py` calls `run_schema_checks()` in its `__main__` guard but discards the returned Boolean. On a handled validation failure, it can log failure yet exit with status 0. The orchestrator instead turns a false result into nonzero status 2 via `sys.exit(main())`. Proposed fix: make standalone validation return a nonzero exit code while preserving its callable Boolean API, and test both invocation paths. **Not yet implemented.**

- **Ingestion failure propagation (observed):** `ingestion.run_ingestion()` logs a missing `data/raw/` directory and returns normally without raising an exception or signaling failure. `orchestrator.run_pipeline()` treats an exception-free return as success, logs ingestion complete, and moves on. Proposed improvement: use a clear failure contract (such as raising `FileNotFoundError` for required input missing) so the orchestrator logs the exception, returns a nonzero exit code, and does not proceed. Also assess handling of zero matching input files. **Not yet implemented.**

- **Empty-batch semantics (design discussion):** An incremental pipeline may legitimately have zero *new* files; in that case a successful no-op with clear logging is preferable to an error. Distinguish this from zero *available/matching* input files on initial setup or a missing input directory, which could indicate misconfiguration. Decide and test the expected contract for full-load versus incremental runs, using explicit input discovery / processed-file state rather than assuming any zero-file case is success. **Policy not finalized; no implementation change.**

## Future UI, API, and deployment integration (planning only)

- **User objective:** Evolve the pipeline into a usable web application as we approach online deployment and GitHub Actions, while preserving the core learning goals in backend, data, and AWS/cloud engineering.
- **Proposed architecture:** Independent ingestion/staging/validation/warehouse/modeling/analytics pipeline -> documented read-only API boundary (FastAPI candidate) -> dashboard/UI. Treat the API as a consumer of approved analytics outputs and pipeline-run metadata, not as a direct browser connection to DuckDB or raw data.
- **Initial UI scope candidates:** Monthly trends, active-loan counts, outstanding principal balance, average interest rate, and pipeline/data-quality run status. Verify the meaning and correctness of each metric before surfacing it.
- **Sequence:** Stabilize pipeline behavior and model semantics; establish tested analytics outputs; design API response contracts; prototype UI (Streamlit vs separate frontend to be decided); then deploy with GitHub Actions testing/deployment automation and an appropriate cloud host.
- **Controls to design:** Authentication/authorization if nonpublic access, safe query/filter limits, logging/monitoring, secrets management, deployment environments, budgets/cost alerts, and the ability to distinguish stale from freshly processed datasets. Any future trigger/reprocess controls require explicit safeguards and auditing.
- **Status:** Planning only. No UI/API/framework, AWS deployment service, hosting budget, or implementation timeline has been finalized. Keep UI scope outside the LP-002 loan-dimension modeling acceptance criteria; organize it as separate follow-on work.

## Open questions and decisions pending

1. Which physical Fannie Mae source files and layout variants are actually present locally?
2. Should acquisition-style and performance-style attributes come from separate source files or a verified enhanced layout?
3. What precise grain and attribute-time semantics should `modeled.dim_loan` use?
4. What is the verified uniqueness key for the performance fact?
5. Which changes belong in LP-002, and which should become separate stabilization/testing tickets?

## Review approach

For each potential change, record:

- **Observation:** What the current source code or data shows.
- **Hypothesis:** A possible explanation or risk, not yet proven.
- **Decision:** The behavior we choose after comparing options.
- **Verification:** The test or evidence proving that the decision works.

## Status

- **Review:** In progress.
- **Implementation:** None of the proposed improvements above are marked complete.
- **Next:** Finish the walkthrough of `validation/checks_schema.py`, then review source data and model grain before refactoring LP-002.
