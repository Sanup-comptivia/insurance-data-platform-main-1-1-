# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Transform All 5 LOBs
# MAGIC Runs Silver transformations for Property, Workers' Comp, Auto, GL, and Umbrella.
# MAGIC
# MAGIC **TRAINEE GUIDE — When to run this notebook:**
# MAGIC Run AFTER `01_run_ingestion.py` has completed successfully. Bronze tables
# MAGIC must exist before Silver can read from them.
# MAGIC
# MAGIC **What the Silver layer does (recap):**
# MAGIC Bronze data is raw strings — Silver makes it usable:
# MAGIC 1. **Union** CSV and JSON Bronze sources for the same LOB into one table
# MAGIC 2. **Standardize** column names (camelCase → snake_case)
# MAGIC 3. **Cast types**: "2024-01-15" → DateType, "1500.00" → DecimalType(18,2)
# MAGIC 4. **Select** only canonical columns (drop Bronze noise/extras)
# MAGIC 5. **Deduplicate**: keep one record per policy_id / claim_id (newest wins)
# MAGIC 6. **DQ flags**: add _is_valid and _dq_issues without dropping any rows
# MAGIC 7. **Write**: overwrite the Silver Delta table
# MAGIC
# MAGIC **Output tables (2 per LOB = 10 total):**
# MAGIC | LOB | Policies Table | Claims Table |
# MAGIC |-----|---------------|--------------|
# MAGIC | Property | silver_property_policies | silver_property_claims |
# MAGIC | Workers' Comp | silver_workers_comp_policies | silver_workers_comp_claims |
# MAGIC | Auto | silver_auto_policies | silver_auto_claims |
# MAGIC | General Liability | silver_general_liability_policies | silver_gl_claims |
# MAGIC | Umbrella | silver_umbrella_policies | silver_umbrella_claims |
# MAGIC
# MAGIC **Next step after this:** Run `05_build_gold_dimensions.py`

# COMMAND ----------

# TRAINEE NOTE — sys.path ensures Python can find the src/ modules.
# This must be done before any `from src.xxx import ...` statements.
import os
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Property Insurance
# MAGIC
# MAGIC **TRAINEE NOTE — What is Property insurance?**
# MAGIC Property insurance covers physical assets: buildings, contents, and
# MAGIC (in this platform) flood damage via the FEMA NFIP program.
# MAGIC
# MAGIC Key property-specific columns in Silver:
# MAGIC - `flood_zone`: FEMA flood zone code (A, AE, X, etc.) — affects premium
# MAGIC - `total_building_coverage`: Max payout for the building structure
# MAGIC - `total_contents_coverage`: Max payout for furniture/belongings
# MAGIC - `occupancy_type`: Residential vs. commercial property
# MAGIC - `construction_type`: Frame, masonry, etc. — affects fire/wind risk
# MAGIC
# MAGIC Source data: synthetic CSV/JSON + real FEMA NFIP bulk data

# COMMAND ----------

from src.silver.transform_property import PropertyTransformer

# PropertyTransformer.transform_all() runs both:
#   - transform_policies() → silver_property_policies
#   - transform_claims()   → silver_property_claims
PropertyTransformer(spark).transform_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Workers' Compensation
# MAGIC
# MAGIC **TRAINEE NOTE — What is Workers' Comp (WC) insurance?**
# MAGIC Workers' Compensation covers employees who are injured on the job.
# MAGIC The employer buys WC insurance; if an employee is hurt at work, WC
# MAGIC pays their medical bills and lost wages.
# MAGIC
# MAGIC Key WC-specific columns:
# MAGIC - `payroll_amount`: The employer's total payroll — this is the EXPOSURE
# MAGIC   BASE used to calculate WC premiums (unlike other LOBs where premium
# MAGIC   is based on the insured value)
# MAGIC - `class_code`: NCCI class code that describes the type of work
# MAGIC   (e.g. 8810 = clerical, 5403 = carpentry) — different codes have
# MAGIC   different risk levels and premium rates
# MAGIC - `experience_mod_factor`: A multiplier applied to the base rate based
# MAGIC   on the employer's own claim history (< 1.0 = good history, > 1.0 = bad)

# COMMAND ----------

from src.silver.transform_workers_comp import WorkersCompTransformer
WorkersCompTransformer(spark).transform_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto Insurance
# MAGIC
# MAGIC **TRAINEE NOTE — What is Auto insurance?**
# MAGIC Auto insurance covers vehicles and liability from vehicle accidents.
# MAGIC Personal auto is one of the most common insurance products.
# MAGIC
# MAGIC Key Auto-specific columns:
# MAGIC - `vin`: Vehicle Identification Number — 17-character unique vehicle ID
# MAGIC - `vehicle_year`, `vehicle_make`, `vehicle_model`: Vehicle details
# MAGIC   (newer, more expensive cars = higher comprehensive/collision premium)
# MAGIC - `liability_limit`: Maximum the insurer pays if the driver injures
# MAGIC   someone else (e.g. 100/300 = $100K per person / $300K per accident)
# MAGIC - `garage_state`: Where the vehicle is normally stored — affects rates
# MAGIC   (urban areas with higher theft/accident rates = higher premiums)
# MAGIC - `deductible_amount`: What the insured pays out-of-pocket before insurance kicks in

# COMMAND ----------

from src.silver.transform_auto import AutoTransformer
AutoTransformer(spark).transform_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. General Liability
# MAGIC
# MAGIC **TRAINEE NOTE — What is General Liability (GL) insurance?**
# MAGIC GL insurance covers a business against claims of bodily injury or
# MAGIC property damage caused to third parties during normal business operations.
# MAGIC Example: A customer slips and falls in a store → GL pays the lawsuit.
# MAGIC
# MAGIC Key GL-specific columns:
# MAGIC - `business_type`: Type of business (Retail, Restaurant, Manufacturing, etc.)
# MAGIC   — different businesses have very different liability exposures
# MAGIC - `revenue`: Annual business revenue — the EXPOSURE BASE for GL premium
# MAGIC   (larger revenue = more customers = more liability exposure)
# MAGIC - `occurrence_limit`: Max payout per single incident (e.g. $1M per slip-and-fall)
# MAGIC - `aggregate_limit`: Max total payout across ALL incidents in a policy year
# MAGIC   (e.g. $2M aggregate means insurer won't pay more than $2M total in a year)

# COMMAND ----------

from src.silver.transform_general_liability import GeneralLiabilityTransformer
GeneralLiabilityTransformer(spark).transform_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Umbrella Insurance
# MAGIC
# MAGIC **TRAINEE NOTE — What is Umbrella insurance?**
# MAGIC Umbrella insurance is EXCESS coverage that sits on top of other policies.
# MAGIC If an Auto, GL, or WC claim exceeds the underlying policy limit,
# MAGIC the Umbrella policy kicks in to cover the remainder up to its own limit.
# MAGIC
# MAGIC Example:
# MAGIC - Auto liability limit: $300K
# MAGIC - A major accident results in a $1.5M lawsuit
# MAGIC - Auto pays $300K (its limit)
# MAGIC - Umbrella pays the remaining $1.2M (up to the umbrella limit)
# MAGIC
# MAGIC Key Umbrella-specific columns:
# MAGIC - `umbrella_limit`: Total excess coverage available (e.g. $5M, $10M)
# MAGIC - `retention_amount`: The "self-insured retention" — amount the insured
# MAGIC   pays before the umbrella kicks in (similar to a deductible)
# MAGIC - `underlying_auto_policy_id`, `underlying_gl_policy_id`, `underlying_wc_policy_id`:
# MAGIC   FKs linking to the primary policies this umbrella covers. An umbrella
# MAGIC   policy requires the insured to maintain specific underlying policies.

# COMMAND ----------

from src.silver.transform_umbrella import UmbrellaTransformer
UmbrellaTransformer(spark).transform_all()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables
# MAGIC
# MAGIC **TRAINEE NOTE — How to read the verification output:**
# MAGIC Each line shows: `table_name: TOTAL rows (VALID rows valid)`
# MAGIC - TOTAL = all rows including DQ-flagged ones
# MAGIC - VALID = rows where _is_valid = True (passed all DQ checks)
# MAGIC
# MAGIC If valid << total, investigate which checks are failing:
# MAGIC ```sql
# MAGIC SELECT _dq_issues, COUNT(*) as cnt
# MAGIC FROM insurance_catalog.silver.silver_property_policies
# MAGIC WHERE NOT _is_valid
# MAGIC GROUP BY _dq_issues ORDER BY cnt DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all Silver tables — confirm expected 10 tables created
# MAGIC SHOW TABLES IN insurance_catalog.silver;

# COMMAND ----------

# Print each Silver table with total and valid row counts.
# TRAINEE NOTE: We dynamically loop over all Silver tables rather than
# hardcoding each table name. This means if new LOBs are added in the future,
# the verification loop automatically picks them up without code changes.
silver_tables = spark.sql("SHOW TABLES IN insurance_catalog.silver").collect()
print(f"\nSilver tables: {len(silver_tables)}")
for t in silver_tables:
    count = spark.table(f"insurance_catalog.silver.{t.tableName}").count()
    # Count valid rows: filter where _is_valid = True
    valid = spark.table(f"insurance_catalog.silver.{t.tableName}").filter("_is_valid = true").count()
    validity_pct = round(valid / count * 100, 1) if count > 0 else 0
    print(f"  {t.tableName}: {count:,} rows ({valid:,} valid = {validity_pct}%)")

print("\nNext step: Run 05_build_gold_dimensions.py")
