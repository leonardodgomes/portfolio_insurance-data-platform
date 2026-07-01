---

## Executive Insights & BI Business Value

The Power BI Executive Dashboard provides immediate corporate clarity with a comprehensive layout optimized for C-level scanning.

### Core Business Discoveries (Vectored Data Realism)
*   **Segment Risk (Auto Line):** The dashboard visually proves the high-risk nature of the youth segment. The **Auto Insurance Loss Ratio spikes at 65%** for the `18-24 (Jovem)` age group, aligning perfectly with market statistics and justifying higher premium pricing for this cohort.
*   **Demographic Shift (Health Line):** Medical claims show an expected biological correlation, with the **Health Insurance Loss Ratio scaling up to 48%** in the `56+ (Sénior)` band.
*   **Geographical Profitability:** While `Lisboa` and `Porto` generate the highest absolute revenue, `Coimbra` and `Faro` stand out as high-margin regions with negligible claim volumes relative to written premiums.

### Enterprise DAX Implementation
The model features decoupled metadata management via an isolated `_measures` table. Core dynamic logic includes:
1.  **Dynamic Status Labeling:** Categorizes performance tiers dynamically using a conditional framework.
2.  **Hex Color Inversion:** Centralizes formatting themes inside the model data layer (`Loss Ratio Color`) to drive responsive visual alerts without manual UI overhead.

---

## How to Deploy and Run Local Replications

### Prerequisites
*   Databricks Workspace (Serverless or All-Purpose Compute Engine)
*   Power BI Desktop (Latest Version)

### Execution Steps
1.  **Ecosystem Environment Setup:** Copy the contents of `requirements.txt` or execute the top cell in the generator file to register the automated environment constraints.
2.  **Data Ingestion Pipeline:** 
    *   Open and trigger `src/data_generator/generate_data.py` inside Databricks using a **Serverless Compute** node. This populates your Unity Catalog isolated Volume layout.
    *   Run `databricks/notebooks/01_bronze_ingest.py` to lock raw historical snapshots.
3.  **Transformation & Orchestration:**
    *   Execute `02_silver_transform.py` and `03_gold_aggregates.py` sequentially or configure them as an automated DAG inside **Lakeflow Jobs** using a time-based cron trigger.
4.  **BI Delivery:**
    *   Open `powerbi/insurance_analytics_dashboard.pbip` via Power BI Desktop.
    *   When prompted, plug in your serverless connection credentials (**Server Hostname** and **HTTP Path**) retrieved from the Databricks SQL Warehouse connection panel.
    *   Authenticate securely using your personal access token (PAT) to load live query streams via **DirectQuery**.
