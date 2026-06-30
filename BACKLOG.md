# Project Backlog - Insurance Data Platform

This file tracks the Agile User Stories and Tasks assigned to the Data Engineering and Analytics team for the **Enterprise Insurance Analytics Platform**.

---

## Sprint 1: Infrastructure & Data Ingestion

### [INS-101] Repository Setup and CI/CD Automation
* **User Story:** As a Data Engineer, I want an automated CI/CD pipeline using Databricks Asset Bundles (DABs) and GitHub Actions so that infrastructural and notebook changes are automatically deployed to our workspaces upon code approval.
* **Tasks:**
  * [x] Design multi-environment repository layout (`dev`/`prod`).
  * [x] Configure `databricks.yml` and resource orchestration definitions.
  * [x] Create GitHub Actions workflow for automated validation and deployment.
  * [x] Establish secure connection using GitHub Repository Secrets.

### [INS-102] Insurance Synthetic Data Generation
* **User Story:** As a QA and Data Engineer, I need a realistic, continuous data generator for customers, policies, and claims so that we can test our pipeline logic against business-accurate correlations without exposing real PII data.
* **Tasks:**
  * [x] Develop Python script utilizing `faker` and `pandas`.
  * [x] Build data distribution logic (e.g., lower credit scores = higher premiums).
  * [x] Implement CDC (Change Data Capture) incremental update simulation.

### [INS-103] Medallion Architecture: Bronze Ingestion Pipeline
* **User Story:** As a Data Engineer, I want to ingest raw landing CSV files into Delta Lake Bronze tables using Auto Loader or standard PySpark streaming to preserve history with minimal processing.
* **Tasks:**
  * [ ] Create notebook `01_bronze_ingest.py`.
  * [ ] Enforce schema and append ingestion metadata (`ingest_time`, `source_file`).

---

## Sprint 2: Data Quality & Business Transformation

### [INS-201] Medallion Architecture: Silver Cleaning & CDC Merge
* **User Story:** As a Data Governance officer, I want claims and policy tables cleaned, deduplicated, and unified so that downstream analysts have a single, trusted source of truth.
* **Tasks:**
  * [ ] Create notebook `02_silver_transform.py`.
  * [ ] Implement `MERGE INTO` (SCD Type 1/2) to handle incremental updates and status changes (e.g., claim moving from *Pending* to *Approved*).
  * [ ] Filter out anomalous entries (negative amounts).

### [INS-202] Medallion Architecture: Gold Business Aggregations
* **User Story:** As an Insurance Business Analyst, I want high-performance aggregates containing daily/monthly KPIs like Loss Ratio and Claim Frequency so that I can generate business intelligence reports without scanning raw tables.
* **Tasks:**
  * [ ] Create notebook `03_gold_aggregates.py`.
  * [ ] Pre-calculate aggregates grouped by `policy_type`, `state`, and `age_group`.

---

## Sprint 3: BI Integration & Enterprise Reporting

### [INS-301] Power BI Semantic Model Setup
* **User Story:** As a BI Developer, I want to connect Power BI safely to the Databricks Gold Layer via a SQL Warehouse so that the executive dashboard displays real-time key metrics.
* **Tasks:**
  * [ ] Configure DirectQuery/Import connection to Databricks.
  * [ ] Implement DAX measures for **Loss Ratio**, **Claim Frequency**, and **Average Claim Cost**.
  * [ ] Save the report structure as a Git-friendly Power BI Project (`.pbip`).
