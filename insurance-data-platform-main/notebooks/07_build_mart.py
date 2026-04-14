# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Build mart_policy_360
# MAGIC Builds the final denormalized data mart: one row per policy_id with 360-degree view.
# MAGIC
# MAGIC **TRAINEE GUIDE — What is the Data Mart and why is it the final step?**
# MAGIC The Data Mart is the FINAL output of the entire pipeline — the table that
# MAGIC business users, BI tools (Power BI, Tableau, Looker), and dashboards use.
# MAGIC
# MAGIC **Why build a mart when we already have the star schema (dims + facts)?**
# MAGIC The star schema requires users to write JOIN queries to combine dims and facts.
# MAGIC This is powerful but complex for non-SQL users and slower for dashboards.
# MAGIC
# MAGIC The mart pre-joins everything and pre-aggregates facts so that:
# MAGIC - Dashboards query ONE table instead of joining 5-8 tables
# MAGIC - Business users can filter/sort without writing SQL joins
# MAGIC - BI tools run faster (no join computation at query time)
# MAGIC - Common KPIs (loss_ratio, total_premium, claim_counts) are pre-computed
# MAGIC
# MAGIC **mart_policy_360 = one wide row per policy containing EVERYTHING:**
# MAGIC ```
# MAGIC Policy identity + LOB + Insured details + Agent details + Location +
# MAGIC Premium totals + Claims totals + Loss ratio + LOB-specific attributes +
# MAGIC Derived fields (is_active, days_until_expiry)
# MAGIC ```
# MAGIC
# MAGIC **Run AFTER:** `06_build_gold_facts.py` (all dims AND facts must exist)
# MAGIC **Run BEFORE:** `03_mart_validation.py` (validate the completed mart)

# COMMAND ----------

import os
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# COMMAND ----------

# TRAINEE NOTE — The mart build is a single function call, but internally it:
#   1. Loads dim_policy as the base (one row per policy)
#   2. LEFT JOINs dim_insured, dim_lob, dim_agent, dim_location (adds context)
#   3. Aggregates fact_premium by policy_key (total premiums per policy)
#   4. Aggregates fact_claim_transaction by policy_key (total claims per policy)
#   5. LEFT JOINs Silver LOB tables for LOB-specific attributes
#   6. Computes derived fields (loss_ratio, is_active, days_until_expiry)
#   7. Writes the wide table to insurance_catalog.gold.mart_policy_360
#
# This is the most compute-intensive step — it touches every Silver and Gold table.
# On a local mode (~1GB) run, expect 3-8 minutes.
# On a databricks mode (~100GB) run, expect 20-60 minutes on a large cluster.
from src.gold.mart.mart_policy_360 import build_mart_policy_360
mart = build_mart_policy_360(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Output
# MAGIC
# MAGIC **TRAINEE NOTE — Reading the mart output:**
# MAGIC Each row in the mart represents ONE complete insurance policy with all its
# MAGIC context pre-joined. Key columns to understand:
# MAGIC - `policy_id`: The policy's natural key (e.g. "AUTO-0000000001")
# MAGIC - `lob_name`: The line of business in plain English
# MAGIC - `insured_name`: Who holds the policy
# MAGIC - `total_written_premium`: Annual premium billed for this policy
# MAGIC - `total_claims_count`: How many claims this policy has had
# MAGIC - `loss_ratio`: Claims / Premium — the key profitability metric
# MAGIC   (below 1.0 = profitable; above 1.0 = underwriting loss)
# MAGIC - `is_active`: True if the policy has not yet expired as of today
# MAGIC
# MAGIC LOB-specific columns (auto_vehicle_make, wc_class_code, etc.) will be
# MAGIC NULL for policies that belong to a different LOB.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the mart — 20 sample policies with key metrics.
# MAGIC -- Try adding WHERE clauses to explore specific LOBs or states:
# MAGIC --   WHERE lob_code = 'AUTO'
# MAGIC --   WHERE policy_state = 'CA'
# MAGIC --   WHERE is_active = true AND loss_ratio > 1.0
# MAGIC SELECT policy_id, lob_name, insured_name, insured_type,
# MAGIC        total_written_premium, total_claims_count, loss_ratio, is_active
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio summary by LOB: premium, claims, and loss ratio.
# MAGIC -- TRAINEE NOTE: This is a typical management dashboard query.
# MAGIC -- It aggregates the mart (which already has pre-computed per-policy metrics)
# MAGIC -- up to the LOB level for a portfolio overview.
# MAGIC --
# MAGIC -- Interpretation:
# MAGIC --   AVG(loss_ratio) = average loss ratio across all policies in the LOB
# MAGIC --   A high avg_loss_ratio for one LOB signals a pricing or underwriting problem
# MAGIC --   for that business line (claims are exceeding what premiums can support).
# MAGIC SELECT lob_name,
# MAGIC        COUNT(*) AS policies,
# MAGIC        ROUND(SUM(total_written_premium), 2) AS total_premium,
# MAGIC        SUM(total_claims_count) AS total_claims,
# MAGIC        ROUND(AVG(loss_ratio), 4) AS avg_loss_ratio
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC GROUP BY lob_name
# MAGIC ORDER BY total_premium DESC;
