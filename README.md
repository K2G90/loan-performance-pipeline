# Loan Performance Data Pipeline
_End-to-End ETL Pipeline for Monthly Loan Performance Data_

This project simulates an enterprise-grade data engineering pipeline for processing monthly loan performance datasets from Fannie Mae’s publicly available Single-Family Loan Performance data. It includes ingestion, staging, automated data quality validation, schema modeling, and an analytics layer powered by SQL.

---

## 🌐 Tech Stack

- **Python** — ETL, validation, pipeline logic  
- **DuckDB** — local OLAP warehouse engine  
- **SQL (CTEs, window functions)**  
- **CSV → Parquet** data transformation  
- **Airflow / Python scheduler (future)**  
- **Pandas & PyArrow** — transformations  
- **Git & GitHub** — version control  
- **Markdown + Diagrams** — documentation  

---

## 📌 Core Features

### 1. Ingestion Layer
- Loads monthly loan performance files (public Fannie Mae dataset)  
- Normalizes column names and types  
- Lands files into a raw data zone (`data/raw/`)

### 2. Staging Layer
- Standardizes dates, strings, identifiers  
- Ensures consistent schemas across monthly files  

### 3. Data Quality Validation
Automated checks for:  
- Expected column presence  
- Data types  
- Null thresholds  
- Value ranges (LTV, interest rate, DTI, etc.)  
- Uniqueness / primary keys  
- Referential integrity between acquisition and performance data  

### 4. Warehouse Modeling (Star Schema)
Modeled tables include:

- `fact_loan_performance`  
- `dim_borrower`  
- `dim_property`  
- `dim_product`  
- `dim_time`  

### 5. Analytics Layer

SQL views and queries for:

- Monthly delinquency rates  
- Risk segmentation by FICO, LTV, DTI  
- Borrower-level trends  
- Loan performance KPIs over time  

---

## 📈 Architecture Diagram

The pipeline architecture diagram will be added under:

`architecture/architecture.png`

---

## 🚀 Future Enhancements

- Convert Python scheduler to Airflow DAGs  
- Add dbt-style transformations  
- Add Great Expectations–style validation framework  
- Add API or dashboard layer on top of the analytics views  

---

## 👤 Author

**Cedric Williams**  
Aspiring Data Engineer | Backend x Big Data | Pipeline Optimization
