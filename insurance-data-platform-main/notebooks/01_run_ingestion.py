# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Data Platform — Data Ingestion Orchestration
# MAGIC Runs synthetic data generation, public data download, and Bronze ingestion.
# MAGIC
# MAGIC **TRAINEE GUIDE — What this notebook does:**
# MAGIC This notebook is Step 2 of the pipeline (run after `00_setup_catalog.py`).
# MAGIC It covers the very first stage of the Medallion Architecture:
# MAGIC ```
# MAGIC Raw Data Sources → [Bronze Layer] → Silver → Gold
# MAGIC ```
# MAGIC
# MAGIC **Four stages run in sequence:**
# MAGIC 1. **Generate Synthetic Data** — Creates realistic fake insurance data using Faker (local) or dbldatagen (Databricks)
# MAGIC 2. **Download Public Data** — Fetches real FEMA NFIP data from public APIs
# MAGIC 3. **Bronze Ingestion (CSV/Parquet)** — Loads structured files into Delta tables
# MAGIC 4. **Bronze Ingestion (JSON/XML)** — Loads semi-structured files into Delta tables
# MAGIC
# MAGIC **After this notebook:** Run `04_run_silver.py` to transform Bronze into Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install Dependencies

# COMMAND ----------

# TRAINEE NOTE — Why %pip install inside the notebook?
# Databricks clusters have many libraries pre-installed, but some packages
# we need (dbldatagen, faker, pyyaml, requests) must be installed per-session.
# %pip install runs pip in the cluster's Python environment.
# Alternative: install libraries permanently on the cluster via the UI
# (Compute → Libraries → Install New). Using %pip is convenient for
# notebooks that may run on different clusters.
#
# Packages explained:
#   dbldatagen : Databricks synthetic data generation (scalable, Spark-native)
#   faker      : Python library for generating realistic fake data (local mode)
#   pyyaml     : Reads our pipeline_config.yaml and data_quality_rules.yaml
#   requests   : HTTP library for downloading FEMA API data
# MAGIC %pip install dbldatagen faker pyyaml requests

# COMMAND ----------

# TRAINEE NOTE — sys.path.insert explains:
# Databricks runs notebooks in a Python environment where the /Workspace path
# is the file system location of your Databricks Workspace files.
# By inserting our project root into sys.path, Python's import system can find
# our custom modules (src/bronze/, src/silver/, etc.) without needing to
# install them as a package.
#
# This path must match the location where you have synced the repository.
# If you clone the repo to a different Workspace folder, update this path.
import os
import sys
from pyspark.sql.utils import AnalysisException
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Synthetic Data
# MAGIC
# MAGIC **TRAINEE NOTE — Why synthetic data?**
# MAGIC Real insurance data is highly sensitive (PII: names, addresses, SSNs).
# MAGIC We use synthetic data to:
# MAGIC - Develop and test the pipeline without exposing real customer data
# MAGIC - Generate data at any scale (local: ~1GB, production: 100-200GB)
# MAGIC - Reproduce results exactly (seeded random generation)
# MAGIC
# MAGIC **Two modes:**
# MAGIC - `local` : Uses Python's Faker library (~100K rows per table, ~1GB total, fast for development)
# MAGIC - `databricks` : Uses dbldatagen (up to 150M rows per table, 100-200GB, production scale)
# MAGIC
# MAGIC **Use the dropdown widget (top of notebook) to choose the mode.**

# COMMAND ----------

from src.ingestion.generate_synthetic import SyntheticDataGenerator

# TRAINEE NOTE — Databricks Widgets
# dbutils.widgets creates interactive controls (dropdowns, text boxes) at the
# top of a notebook. This lets users choose the mode without editing code.
# dbutils.widgets.get() reads the currently selected value.
# In production pipelines, the mode is usually set via job parameters instead.
dbutils.widgets.dropdown("mode", "local", ["local", "databricks"], "Generation Mode")
mode = dbutils.widgets.get("mode")

# SyntheticDataGenerator reads the mode and uses the appropriate row counts
# from pipeline_config.yaml (local: 100K rows, databricks: 150M rows).
generator = SyntheticDataGenerator(spark, mode=mode)
generator.generate_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Download Public Data Sources
# MAGIC
# MAGIC **TRAINEE NOTE — What public data are we downloading?**
# MAGIC - **FEMA NFIP** (National Flood Insurance Program):
# MAGIC   - 2.7M real flood insurance claims (publicly available)
# MAGIC   - 72M real flood insurance policies
# MAGIC   - Source: https://www.fema.gov/flood-insurance/work-with-nfip/data-visualizations
# MAGIC - **CAS (Casualty Actuarial Society)**: Actuarial loss triangles for WC, Auto, GL
# MAGIC
# MAGIC **Note:** This step requires internet access from the Databricks cluster.
# MAGIC If behind a firewall, the download may fail — in that case, skip this step
# MAGIC and use only the synthetic data generated in Step 1.

# COMMAND ----------

from src.ingestion.download_public_data import PublicDataDownloader

downloader = PublicDataDownloader(spark)
downloader.download_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Layer Ingestion
# MAGIC
# MAGIC **TRAINEE NOTE — What happens during Bronze ingestion?**
# MAGIC The generated/downloaded files are loaded into Delta tables in Unity Catalog.
# MAGIC Key principles of Bronze ingestion:
# MAGIC
# MAGIC 1. **No transformations**: Data lands exactly as-is (no type casting, no cleaning)
# MAGIC 2. **Schema-on-read for CSV**: All columns stored as strings
# MAGIC 3. **Metadata added**: Every row gets `_ingestion_timestamp`, `_source_file`,
# MAGIC    `_source_format`, and `_batch_id` for full audit trail
# MAGIC 4. **Overwrite mode**: Safe to re-run — replaces previous load
# MAGIC
# MAGIC **Two classes handle different formats:**
# MAGIC - `BronzeStructuredIngestion` → CSV and Parquet files
# MAGIC - `BronzeSemiStructuredIngestion` → JSON and XML files

# COMMAND ----------

from src.bronze.ingest_structured import BronzeStructuredIngestion
from src.bronze.ingest_semi_structured import BronzeSemiStructuredIngestion

# Structured ingestion (CSV/Parquet)
# Reads 10 sources: synthetic CSVs + FEMA bulk CSVs for all 5 LOBs
structured = BronzeStructuredIngestion(spark)
structured.ingest_all()

# Semi-structured ingestion (JSON/XML)
# Reads 8 sources: synthetic JSONs + FEMA API JSONs for all 5 LOBs
semi = BronzeSemiStructuredIngestion(spark)
semi.ingest_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Bronze Tables
# MAGIC
# MAGIC **TRAINEE NOTE:**
# MAGIC After ingestion, we verify the Bronze tables were created and contain data.
# MAGIC A missing or empty table usually means:
# MAGIC - The source file path doesn't exist (check `raw_base_path` configuration)
# MAGIC - The file format doesn't match (CSV options, JSON structure)
# MAGIC - The synthetic data generation step failed
# MAGIC
# MAGIC The SQL command below lists all tables in the bronze schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables currently in the Bronze schema
# MAGIC -- Expected: ~18 tables (10 CSV + 8 JSON sources)
# MAGIC SHOW TABLES IN insurance_catalog.bronze;

# COMMAND ----------

# Programmatic verification: print each table name and its row count.
# TRAINEE NOTE: We catch AnalysisException specifically for TABLE_OR_VIEW_NOT_FOUND
# rather than using a bare except. This way:
#   - Missing tables print a warning and we continue
#   - Unexpected errors (cluster issues, permissions) are re-raised so we notice them
bronze_tables = spark.sql("SHOW TABLES IN insurance_catalog.bronze").collect()
print(f"\nBronze tables created: {len(bronze_tables)}")

for t in bronze_tables:
    try:
        count = spark.table(f"insurance_catalog.bronze.{t.tableName}").count()
        print(f"  {t.tableName}: {count:,} rows")
    except AnalysisException as ae:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(ae):
            print(f"  {t.tableName}: Table not found")
        else:
            raise ae  # Re-raise unexpected exceptions

print("\nIngestion complete!")
print("Next step: Run 04_run_silver.py to transform Bronze → Silver")
