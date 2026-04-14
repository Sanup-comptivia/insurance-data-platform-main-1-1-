# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Data Platform — Run All Pipeline
# MAGIC
# MAGIC **One-click execution** of the entire Medallion pipeline:
# MAGIC 1. Setup Catalog (Bronze/Silver/Gold schemas)
# MAGIC 2. Generate Synthetic Data + Bronze Ingestion
# MAGIC 3. Silver Transformations (5 LOBs)
# MAGIC 4. Gold Dimensions (8 tables)
# MAGIC 5. Gold Facts (3 tables)
# MAGIC 6. Data Mart (mart_policy_360)
# MAGIC 7. Validation
# MAGIC
# MAGIC **TRAINEE GUIDE — How to use this notebook:**
# MAGIC - Click **"Run All"** at the top to execute the entire pipeline end-to-end
# MAGIC - Or run each cell individually to step through and inspect intermediate results
# MAGIC - Check the print output after each step — it shows table names and row counts
# MAGIC - If a step fails, you can re-run from that step (most steps are idempotent)
# MAGIC
# MAGIC **Estimated runtime:**
# MAGIC - `local` mode (~1GB):  10-20 minutes on a small cluster
# MAGIC - `databricks` mode (~100-200GB): 60-120 minutes on a large cluster
# MAGIC
# MAGIC **Architecture reminder:**
# MAGIC ```
# MAGIC Raw Data → Bronze → Silver → Gold Dims → Gold Facts → Mart
# MAGIC              ↑          ↑           ↑           ↑          ↑
# MAGIC         Step 2-3   Step 4      Step 5       Step 6    Step 7
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **TRAINEE NOTE — Change `GENERATION_MODE` here before running:**
# MAGIC - `"local"` : Uses Faker, generates ~100K rows per table (~1GB total)
# MAGIC               Best for first runs, testing, and development
# MAGIC - `"databricks"` : Uses dbldatagen, generates up to 150M rows per table
# MAGIC                    Best for production-scale testing and performance validation

# COMMAND ----------

# Add project root to Python path so imports from src/ work correctly
import os
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# TRAINEE NOTE: Change this to "databricks" when running a full production-scale test
GENERATION_MODE = "local"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Setup Unity Catalog
# MAGIC
# MAGIC **TRAINEE NOTE — Prerequisites before running this step:**
# MAGIC 1. The `insurance_catalog` must be created FIRST via the Databricks UI:
# MAGIC    Catalog Explorer → + Add → Add a catalog → Name: `insurance_catalog`
# MAGIC 2. You must have CREATE SCHEMA privilege on `insurance_catalog`
# MAGIC 3. The catalog is created once; subsequent runs skip this step (IF NOT EXISTS)
# MAGIC
# MAGIC **Three schemas (databases) are created:**
# MAGIC - `bronze` : Raw landing zone — holds data exactly as received from sources
# MAGIC - `silver` : Cleansed and typed — data ready for business rules and DQ
# MAGIC - `gold`   : Analytics-ready — star schema dims + facts + data mart
# MAGIC
# MAGIC These three schemas represent the three "medals" in the Medallion Architecture.

# COMMAND ----------

# TRAINEE NOTE: USE CATALOG sets the default catalog for subsequent SQL commands.
# Without this, SQL commands would need fully qualified names (catalog.schema.table).
spark.sql("USE CATALOG insurance_catalog")
print("Using catalog: insurance_catalog")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Raw landing zone: data lands here with no transformation
# MAGIC -- IF NOT EXISTS: safe to re-run; won't fail if schema already exists
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.bronze
# MAGIC COMMENT 'Raw landing zone';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleansed and typed data: types cast, DQ flags added, deduplication done
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.silver
# MAGIC COMMENT 'Cleansed and typed data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analytics-ready: star schema (dim + fact tables) + denormalized data mart
# MAGIC CREATE SCHEMA IF NOT EXISTS insurance_catalog.gold
# MAGIC COMMENT 'Business-ready star schema and data mart';

# COMMAND ----------

print("Step 1 COMPLETE: Unity Catalog schemas created")
# Show all schemas to confirm creation was successful
spark.sql("SHOW SCHEMAS IN insurance_catalog").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Generate Synthetic Data
# MAGIC
# MAGIC **TRAINEE NOTE — What data is generated?**
# MAGIC Synthetic data is created for all 5 Lines of Business (LOBs):
# MAGIC - Property (PROP): policies + claims with FEMA-style fields (flood_zone, etc.)
# MAGIC - Workers' Comp (WC): policies + claims with employer/payroll fields
# MAGIC - Auto (AUTO): policies + claims with vehicle details (VIN, make, model)
# MAGIC - General Liability (GL): policies + claims with business info
# MAGIC - Umbrella (UMB): policies + claims (excess/umbrella coverage)
# MAGIC
# MAGIC Data is written to `/mnt/insurance-data/raw/` in cloud storage as files
# MAGIC (CSV and JSON), then Bronze ingestion reads those files into Delta tables.
# MAGIC
# MAGIC The generator is seeded for reproducibility — same mode always produces
# MAGIC the same data. This makes test results comparable between runs.

# COMMAND ----------

from src.ingestion.generate_synthetic import SyntheticDataGenerator

generator = SyntheticDataGenerator(spark, mode=GENERATION_MODE)
generator.generate_all()

# COMMAND ----------

print("Step 2 COMPLETE: Synthetic data generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Bronze Layer Ingestion
# MAGIC
# MAGIC **TRAINEE NOTE — Two separate ingesters run here:**
# MAGIC - `BronzeStructuredIngestion`: Reads CSV/Parquet files, stores as strings
# MAGIC - `BronzeSemiStructuredIngestion`: Reads JSON/XML, handles nested structures
# MAGIC
# MAGIC **After this step you should have ~18 Bronze Delta tables.**
# MAGIC All tables will have 4 metadata columns added:
# MAGIC - `_ingestion_timestamp`: When the row was loaded
# MAGIC - `_source_file`: The file path it came from
# MAGIC - `_source_format`: csv / json / xml
# MAGIC - `_batch_id`: UUID grouping all rows from this specific load run
# MAGIC
# MAGIC **Key property of Bronze:** Every run OVERWRITES the previous data.
# MAGIC This is intentional — Bronze always reflects the most recent file load.

# COMMAND ----------

from src.bronze.ingest_structured import BronzeStructuredIngestion
from src.bronze.ingest_semi_structured import BronzeSemiStructuredIngestion

BronzeStructuredIngestion(spark).ingest_all()
BronzeSemiStructuredIngestion(spark).ingest_all()

# COMMAND ----------

bronze_tables = spark.sql("SHOW TABLES IN insurance_catalog.bronze").collect()
print(f"Step 3 COMPLETE: {len(bronze_tables)} Bronze tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Silver Layer Transformations
# MAGIC
# MAGIC **TRAINEE NOTE — What happens in Silver?**
# MAGIC Silver is the "make it useful" layer. For EACH LOB:
# MAGIC 1. Union CSV + JSON Bronze sources into one table
# MAGIC 2. Standardize column names (camelCase → snake_case)
# MAGIC 3. Cast types: "2024-01-15" → date, "1500.00" → Decimal(18,2)
# MAGIC 4. Select only the canonical columns we need (drop raw noise)
# MAGIC 5. Deduplicate: keep newest record per policy_id / claim_id
# MAGIC 6. Apply DQ checks: flag bad rows with _is_valid and _dq_issues
# MAGIC 7. Write to Silver Delta table
# MAGIC
# MAGIC **5 transformers run independently** (one per LOB):
# MAGIC - PropertyTransformer → silver_property_policies, silver_property_claims
# MAGIC - WorkersCompTransformer → silver_workers_comp_policies, silver_workers_comp_claims
# MAGIC - AutoTransformer → silver_auto_policies, silver_auto_claims
# MAGIC - GeneralLiabilityTransformer → silver_general_liability_policies, silver_gl_claims
# MAGIC - UmbrellaTransformer → silver_umbrella_policies, silver_umbrella_claims
# MAGIC
# MAGIC **After this step you should have ~10 Silver Delta tables (2 per LOB).**

# COMMAND ----------

from src.silver.transform_property import PropertyTransformer
from src.silver.transform_workers_comp import WorkersCompTransformer
from src.silver.transform_auto import AutoTransformer
from src.silver.transform_general_liability import GeneralLiabilityTransformer
from src.silver.transform_umbrella import UmbrellaTransformer

# TRAINEE NOTE: Each transformer is independent — they don't depend on each other.
# In a production pipeline with many LOBs, these could run in PARALLEL using
# Databricks multi-task jobs or Python threads to reduce wall-clock time.
# For simplicity, we run them sequentially here.
PropertyTransformer(spark).transform_all()
WorkersCompTransformer(spark).transform_all()
AutoTransformer(spark).transform_all()
GeneralLiabilityTransformer(spark).transform_all()
UmbrellaTransformer(spark).transform_all()

# COMMAND ----------

silver_tables = spark.sql("SHOW TABLES IN insurance_catalog.silver").collect()
print(f"Step 4 COMPLETE: {len(silver_tables)} Silver tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Gold Layer — Dimensions
# MAGIC
# MAGIC **TRAINEE NOTE — What are Gold Dimension tables?**
# MAGIC Dimensions describe the business entities in our data:
# MAGIC - **dim_date**: Calendar dimension (every date from 1970 to 2035)
# MAGIC - **dim_line_of_business**: The 5 insurance LOBs (PROP, AUTO, WC, GL, UMB)
# MAGIC - **dim_insured**: People and businesses who hold insurance policies
# MAGIC - **dim_location**: Geographic locations of insured risks
# MAGIC - **dim_agent**: Insurance agents who sell policies
# MAGIC - **dim_coverage**: Types of coverage (building, bodily injury, medical, etc.)
# MAGIC - **dim_policy**: Central policy hub — links all other dimensions together
# MAGIC - **dim_claim**: Insurance claims filed against policies
# MAGIC
# MAGIC **Build order matters!** Dimensions must be built in dependency order:
# MAGIC 1. `dim_date`, `dim_line_of_business` → no dependencies (standalone)
# MAGIC 2. `dim_insured`, `dim_location`, `dim_agent`, `dim_coverage` → no dependencies
# MAGIC 3. `dim_policy` → depends on dim_lob, dim_insured, dim_agent (needs their surrogate keys)
# MAGIC 4. `dim_claim` → depends on dim_policy (needs policy_key)
# MAGIC
# MAGIC Each dimension has a surrogate key (MD5 hash) as its primary key and
# MAGIC the business natural key stored separately for traceability.

# COMMAND ----------

from src.gold.dimensions.dim_date import build_dim_date
from src.gold.dimensions.dim_line_of_business import build_dim_line_of_business
from src.gold.dimensions.dim_insured import build_dim_insured
from src.gold.dimensions.dim_location import build_dim_location
from src.gold.dimensions.dim_agent import build_dim_agent
from src.gold.dimensions.dim_coverage import build_dim_coverage
from src.gold.dimensions.dim_policy import build_dim_policy
from src.gold.dimensions.dim_claim import build_dim_claim

# Build in dependency order — each step may depend on outputs of previous steps
build_dim_date(spark)                 # Standalone: no dependencies
build_dim_line_of_business(spark)     # Standalone: LOBs are static reference data
build_dim_insured(spark)              # Standalone: derived from Silver LOB policies
build_dim_location(spark)             # Standalone: geocoded locations
build_dim_agent(spark)                # Standalone: agent reference data
build_dim_coverage(spark)             # Standalone: coverage type reference data
build_dim_policy(spark)               # Depends on: dim_lob, dim_insured, dim_agent
build_dim_claim(spark)                # Depends on: dim_policy

# COMMAND ----------

print("Step 5 COMPLETE: 8 dimension tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Gold Layer — Facts
# MAGIC
# MAGIC **TRAINEE NOTE — What are Gold Fact tables?**
# MAGIC Facts store measurable business transactions and events:
# MAGIC - **fact_premium**: One row per policy-coverage combination
# MAGIC   - Measures: written_premium, earned_premium, ceded_premium, net_premium
# MAGIC   - Links: policy_key, coverage_key, date_key, lob_key, location_key
# MAGIC
# MAGIC - **fact_claim_transaction**: One row per claim payment/reserve transaction
# MAGIC   - Measures: paid_amount, reserve_amount, total_incurred
# MAGIC   - Links: claim_key, policy_key, date_key, lob_key
# MAGIC
# MAGIC - **fact_policy_transaction**: One row per policy lifecycle event
# MAGIC   - Measures: transaction amounts, changes in coverage
# MAGIC   - Links: policy_key, date_key
# MAGIC
# MAGIC **Build order:** Facts must be built AFTER their dimension dependencies.
# MAGIC All dimension tables must exist before building facts.

# COMMAND ----------

from src.gold.facts.fact_premium import build_fact_premium
from src.gold.facts.fact_claim_transaction import build_fact_claim_transaction
from src.gold.facts.fact_policy_transaction import build_fact_policy_transaction

# TRAINEE NOTE: Facts depend on dimensions (they join dims to resolve FKs).
# If a dimension is missing, the FK column will be null but the fact will still
# build (graceful degradation). Check for null FKs if joins look wrong.
build_fact_premium(spark)
build_fact_claim_transaction(spark)
build_fact_policy_transaction(spark)

# COMMAND ----------

print("Step 6 COMPLETE: 3 fact tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Gold Layer — Data Mart
# MAGIC
# MAGIC **TRAINEE NOTE — What is the mart_policy_360?**
# MAGIC The mart is the FINAL output of the pipeline — the table that business
# MAGIC users, BI tools, and dashboards query directly.
# MAGIC
# MAGIC It is a wide, denormalised table with ~65 columns:
# MAGIC - Policy identity (id, number, status, term)
# MAGIC - Who is insured (name, type, DOB, industry)
# MAGIC - Which agent sold it (name, agency, commission tier)
# MAGIC - Where the risk is (address, city, state, lat/long)
# MAGIC - Financial summary (total_written_premium, total_earned_premium)
# MAGIC - Claims summary (total_claims, open_claims, total_paid, loss_ratio)
# MAGIC - LOB-specific details (flood_zone, vehicle_year, payroll, etc.)
# MAGIC - Derived fields (days_until_expiry, is_active)
# MAGIC
# MAGIC **Loss Ratio** is the KEY insurance KPI computed here:
# MAGIC `loss_ratio = total_incurred / total_written_premium`
# MAGIC - < 0.60: Very profitable
# MAGIC - 0.60-0.70: Target range
# MAGIC - > 1.00: Underwriting loss

# COMMAND ----------

from src.gold.mart.mart_policy_360 import build_mart_policy_360
build_mart_policy_360(spark)

# COMMAND ----------

print("Step 7 COMPLETE: mart_policy_360 created")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Validation & Summary
# MAGIC
# MAGIC **TRAINEE NOTE — What to check after a successful pipeline run:**
# MAGIC 1. Row counts: Bronze should have the most rows (all raw data). Silver
# MAGIC    should have slightly fewer (deduplication). Gold dimensions/facts should
# MAGIC    reflect the correct grain. The mart should have one row per policy.
# MAGIC 2. Check for null keys in Gold: A high % of null policy_keys in facts
# MAGIC    indicates a problem in dim_policy build.
# MAGIC 3. Check _is_valid rates in Silver: Run quality summary queries to see
# MAGIC    what % of records are flagged as invalid and why.
# MAGIC 4. Loss ratio sanity check: For synthetic data, expect 0.4-0.8 range.

# COMMAND ----------

# Print a summary table for every layer showing table names and row counts.
# TRAINEE NOTE: This is a quick health check. In production you would add:
#   - Minimum row count thresholds (alert if a table is suspiciously empty)
#   - Data freshness checks (_ingestion_timestamp within expected window)
#   - DQ pass rate checks (% of _is_valid=True rows in Silver tables)
print("=" * 70)
print("INSURANCE DATA PLATFORM — PIPELINE EXECUTION SUMMARY")
print("=" * 70)

for layer, schema in [("BRONZE", "bronze"), ("SILVER", "silver"), ("GOLD", "gold")]:
    tables = spark.sql(f"SHOW TABLES IN insurance_catalog.{schema}").collect()
    print(f"\n{layer} Layer ({len(tables)} tables):")
    for t in tables:
        count = spark.table(f"insurance_catalog.{schema}.{t.tableName}").count()
        print(f"  {t.tableName}: {count:,} rows")

print("\n" + "=" * 70)
print("PIPELINE COMPLETE — ALL LAYERS BUILT SUCCESSFULLY")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Query the Mart
# MAGIC
# MAGIC **TRAINEE NOTE — Sample queries to explore the mart:**
# MAGIC The mart is the primary interface for business analysis. Run the query
# MAGIC below to see 10 policies with their key metrics in one row each.
# MAGIC
# MAGIC Try these follow-up queries:
# MAGIC ```sql
# MAGIC -- Top 10 most expensive claims
# MAGIC SELECT policy_id, insured_name, lob_name, total_paid_amount
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC ORDER BY total_paid_amount DESC LIMIT 10;
# MAGIC
# MAGIC -- Premium by state and LOB
# MAGIC SELECT policy_state, lob_name, COUNT(*) as policies,
# MAGIC        SUM(total_written_premium) as total_premium
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC GROUP BY policy_state, lob_name
# MAGIC ORDER BY total_premium DESC;
# MAGIC
# MAGIC -- High loss ratio policies (potential problem accounts)
# MAGIC SELECT policy_id, insured_name, lob_name, loss_ratio, total_claims_count
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC WHERE loss_ratio > 1.0
# MAGIC ORDER BY loss_ratio DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Look up any policy — see everything about it in one row
# MAGIC SELECT policy_id, lob_name, insured_name,
# MAGIC        total_written_premium, total_claims_count, loss_ratio, is_active
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC LIMIT 10;
