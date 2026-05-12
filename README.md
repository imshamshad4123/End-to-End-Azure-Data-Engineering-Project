# 🚕 NYC Taxi Data Pipeline — End-to-End Azure Data Engineering Project

> A production-style medallion architecture pipeline built on **Azure Data Factory**, 
> **ADLS Gen2**, and **Databricks Unity Catalog** — ingesting all 12 months of 
> NYC TLC Trip Record Data (2025) through Bronze → Silver → Gold layers.

---

## 🏗️ Architecture

```
[NYC TLC Source]  →  [Azure Data Factory]  →  [ADLS Gen2 Raw]
  cloudfront.net     ForEach + If-Else           Parquet files
                     Copy Data Activity
                            ↓
                  [Databricks Unity Catalog]
                  ┌─────────────────────────┐
                  │  Bronze  (Streaming)    │  Raw Delta tables
                  │  Silver  (Refined)      │  Cleaned + typed
                  │  Gold    (Aggregated)   │  Business metrics
                  └─────────────────────────┘
```

---

## 🔧 Tech Stack

| Tool | Usage |
|---|---|
| Azure Data Factory | Orchestration, dynamic ingestion |
| ADLS Gen2 | Raw data landing zone |
| Azure Databricks | Spark processing, Delta Lake |
| Unity Catalog | Governance, external locations |
| PySpark / Spark SQL | Transformations |
| Delta Lake | Managed + external tables |

---

## 📦 Pipeline Breakdown

### 1. Ingestion — Azure Data Factory
- Dynamic pipeline using **ForEach loop** to iterate over all 12 months
- **If-Else condition** to handle file availability checks before copy
- **Copy Data activity** pulling Parquet files from `d37ci6vzurychx.cloudfront.net`
- Destination: ADLS Gen2 raw container

### 2. Unity Catalog Setup
- Created **storage credential** and **external location** pointing to ADLS Gen2
- Registered **external schemas** for Bronze and Silver layers
- Used **managed schema** for Gold layer (Databricks-managed storage)

### 3. Bronze Layer — Raw Ingestion
- **Spark Structured Streaming** query to continuously load from raw zone
- Schema-on-read with minimal transformation
- Created both **managed Delta table** and **external Delta table** for comparison
- Full historical load: ~37M+ rows across 12 months

### 4. Silver Layer — Data Refinement
- Null handling on critical columns (`pickup_datetime`, `dropoff_datetime`, `trip_distance`)
- Type casting: timestamps, floats, integers
- Removed outliers: trips with `trip_distance <= 0`, `fare_amount < 0`
- Deduplication on primary key candidates
- Derived column: `trip_duration_minutes`
- Filter: valid passenger count (1–6)

### 5. Gold Layer — Business Metrics
- Average fare per pickup zone
- Trip volume by hour of day
- Revenue trends by month
- Top 10 busiest pickup locations
- All output as Delta tables ready for BI/reporting

---

## 📂 Repo Structure

```
├── adf_pipelines/
│   └── nyc_taxi_ingest_pipeline.json
├── notebooks/
│   ├── 01_bronze_streaming_ingest.py
│   ├── 02_silver_cleaning.py
│   └── 03_gold_aggregations.py
├── setup/
│   └── unity_catalog_setup.sql
└── README.md
```

---

## 💡 Key Concepts Demonstrated

- ✅ Medallion architecture (Bronze / Silver / Gold)
- ✅ Spark Structured Streaming for incremental loads
- ✅ Unity Catalog with external locations and storage credentials
- ✅ Managed vs external Delta tables
- ✅ Dynamic ADF pipelines (ForEach, If-Else, parameterisation)
- ✅ Delta Lake ACID transactions and time travel capability

---

## 📊 Dataset

**Source:** NYC TLC Trip Record Data 2025 (Yellow Taxi)  
**Volume:** ~37M rows, 12 monthly Parquet files  
**Columns:** pickup/dropoff times, locations, distances, fares, payment type
