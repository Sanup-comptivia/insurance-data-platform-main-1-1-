# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Data Platform — Unity Catalog Setup
# MAGIC Creates the catalog schemas and grants for the Insurance Data Platform.
# MAGIC
# MAGIC **TRAINEE GUIDE — Run this notebook FIRST, before any other notebook.**
# MAGIC
# MAGIC **What is Unity Catalog?**
# MAGIC Unity Catalog is Databricks' centralised governance layer for data.
# MAGIC It uses a three-level namespace: `catalog.schema.table`
# MAGIC - **Catalog** = the top-level container (like a database server)
# MAGIC - **Schema** = a logical grouping of tables (like a database)
# MAGIC - **Table** = the actual Delta table holding data
# MAGIC
# MAGIC **Our three schemas follow the Medallion Architecture:**
# MAGIC ```
# MAGIC insurance_catalog
# MAGIC ├── bronze   ← Raw landing zone: data arrives here first, untouched
# MAGIC ├── silver   ← Cleansed, typed, deduplicated, DQ-flagged data
# MAGIC └── gold     ← Analytics-ready star schema + data mart
# MAGIC ```
# MAGIC
# MAGIC **IMPORTANT prerequisite:**
# MAGIC The catalog `insurance_catalog` must be created via the Databricks UI BEFORE
# MAGIC running this notebook:
# MAGIC > Catalog Explorer → + Add → Add a catalog → Name: `insurance_catalog`
# MAGIC
# MAGIC On most Databricks workspaces, `CREATE CATALOG` via SQL is restricted to
# MAGIC admins. Creating it through the UI is the standard approach for trainees.

# COMMAND ----------

# TRAINEE NOTE — USE CATALOG sets the default catalog for the session.
# After this, all SQL commands that reference a schema (e.g. bronze, silver)
# will automatically use insurance_catalog.bronze, insurance_catalog.silver, etc.
# Without this, you would need to write the full three-part name every time.
spark.sql("USE CATALOG insurance_catalog")
print("Using catalog: insurance_catalog")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Bronze schema.
# MAGIC -- COMMENT is optional metadata stored in the catalog — useful for
# MAGIC -- data discovery tools (Unity Catalog Explorer, data lineage tools).
# MAGIC -- IF NOT EXISTS: safe to re-run — won't throw an error if already created.
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.bronze
# MAGIC COMMENT 'Raw landing zone - no transformations applied';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Silver schema.
# MAGIC -- Silver holds data that has been cleansed, typed, and quality-checked.
# MAGIC -- DQ flags (_is_valid, _dq_issues) are present on every Silver table.
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.silver
# MAGIC COMMENT 'Cleansed, typed, deduplicated data with DQ flags';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Gold schema.
# MAGIC -- Gold holds the star schema (dim + fact tables) and the data mart.
# MAGIC -- This is what BI tools and dashboards connect to.
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.gold
# MAGIC COMMENT 'Normalized star schema dimensions, facts, and data mart';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks Serverless does not allow arbitrary writes to /mnt or DBFS paths.
# MAGIC -- We create Unity Catalog Volumes instead and store raw files/checkpoints there.
# MAGIC CREATE VOLUME IF NOT EXISTS insurance_catalog.bronze.raw_data
# MAGIC COMMENT 'Raw landing-zone files for synthetic and public source data';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS insurance_catalog.bronze.checkpoints
# MAGIC COMMENT 'Checkpoint files for Bronze, Silver, and Gold processing';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify all three schemas were created successfully.
# MAGIC -- Expected output: bronze, silver, gold (plus any pre-existing schemas like 'default')
# MAGIC SHOW SCHEMAS IN insurance_catalog;

# COMMAND ----------

# Python-based verification — prints each schema name for confirmation.
# TRAINEE NOTE: We query schemas again in Python (not just SQL) to show that
# both %sql cells and spark.sql() calls can be used interchangeably in notebooks.
# In production pipeline code (src/ modules) we always use spark.sql() or
# PySpark DataFrame API, never %sql (which is notebook-only syntax).
spark.sql("USE CATALOG insurance_catalog")
schemas = spark.sql("SHOW SCHEMAS").collect()
print("Schemas created:")
for s in schemas:
    print(f"  - {s.databaseName}")

print("\nUnity Catalog setup complete!")
print("Next step: Run 01_run_ingestion.py to generate and ingest source data.")
