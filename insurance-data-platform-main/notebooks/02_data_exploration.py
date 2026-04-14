# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Data Platform — Data Exploration
# MAGIC Explore and profile data across Bronze, Silver, and Gold layers.
# MAGIC
# MAGIC **TRAINEE GUIDE — When to use this notebook:**
# MAGIC Run this notebook AFTER the pipeline has been executed (at least through Silver)
# MAGIC to understand what data was produced and whether it looks correct.
# MAGIC
# MAGIC **What this notebook covers:**
# MAGIC 1. **Bronze Layer** — Row counts per table to confirm ingestion volume
# MAGIC 2. **Silver Layer** — Data quality pass rates (_is_valid percentages)
# MAGIC 3. **Gold Layer** — Dimension and fact row counts
# MAGIC 4. **Premium Distribution** — Revenue breakdown by Line of Business
# MAGIC 5. **Claims Summary** — Loss metrics by Line of Business
# MAGIC
# MAGIC **How to read the output:**
# MAGIC - Bronze counts should be HIGHEST (all raw data, including duplicates)
# MAGIC - Silver counts should be slightly LOWER (deduplication applied)
# MAGIC - Gold dim_policy count should equal mart_policy_360 count (grain = 1 policy)
# MAGIC - validity_pct should be > 95% for healthy data (check _dq_issues if lower)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Summary
# MAGIC
# MAGIC **TRAINEE NOTE — What to look for:**
# MAGIC - Each Bronze table should have rows (zero = ingestion failed for that source)
# MAGIC - Compare CSV and JSON tables for the same LOB — counts may differ slightly
# MAGIC   because they come from different data sources (synthetic vs. FEMA API)
# MAGIC - The FEMA tables (bronze_property_fema_*) may have very high counts
# MAGIC   because FEMA has millions of real policy/claim records

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count records per Bronze table and order alphabetically.
# MAGIC -- UNION ALL combines multiple SELECT results into one output — useful
# MAGIC -- for building summary tables without a GROUP BY.
# MAGIC -- TRAINEE NOTE: Each SELECT reads a different table — this query runs
# MAGIC -- 6 table scans. For a large number of tables, a dynamic query using
# MAGIC -- INFORMATION_SCHEMA or a Python loop would be more maintainable.
# MAGIC SELECT 'bronze_property_policies_csv' AS table_name, COUNT(*) AS row_count FROM insurance_catalog.bronze.bronze_property_policies_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_property_claims_csv', COUNT(*) FROM insurance_catalog.bronze.bronze_property_claims_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_workers_comp_policies_csv', COUNT(*) FROM insurance_catalog.bronze.bronze_workers_comp_policies_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_auto_policies_csv', COUNT(*) FROM insurance_catalog.bronze.bronze_auto_policies_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_general_liability_policies_csv', COUNT(*) FROM insurance_catalog.bronze.bronze_general_liability_policies_csv
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_umbrella_policies_csv', COUNT(*) FROM insurance_catalog.bronze.bronze_umbrella_policies_csv
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Data Quality Summary
# MAGIC
# MAGIC **TRAINEE NOTE — Understanding the DQ columns:**
# MAGIC Every Silver table has two DQ columns added by `DataQualityChecker.apply()`:
# MAGIC - `_is_valid` (boolean): True = all DQ checks passed; False = at least one failed
# MAGIC - `_dq_issues` (string): Semicolon-separated list of failed check names
# MAGIC
# MAGIC **The query below computes:**
# MAGIC - `total`: All rows in the Silver table (valid + invalid)
# MAGIC - `valid`: Rows where _is_valid = True
# MAGIC - `validity_pct`: The percentage of rows passing all DQ checks
# MAGIC
# MAGIC **Interpreting validity_pct:**
# MAGIC - 100%: Perfect — all rows pass every check
# MAGIC - 95-99%: Normal — some edge cases but acceptable
# MAGIC - < 90%: Investigate — run `SELECT _dq_issues, COUNT(*) FROM silver_table WHERE NOT _is_valid GROUP BY _dq_issues`
# MAGIC - < 50%: Critical — likely a schema mismatch or source data problem

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DQ summary per Silver table.
# MAGIC -- CASE WHEN _is_valid THEN 1 ELSE 0 END converts boolean to 0/1 for SUM.
# MAGIC -- This pattern (SUM of a CASE WHEN) is called a "conditional count" —
# MAGIC -- it's equivalent to COUNT(*) WHERE _is_valid = true but works inside
# MAGIC -- a single GROUP BY query.
# MAGIC -- ROUND(..., 2) gives two decimal places in the percentage.
# MAGIC SELECT
# MAGIC   'silver_property_policies' AS table_name,
# MAGIC   COUNT(*) AS total,
# MAGIC   SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END) AS valid,
# MAGIC   ROUND(SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS validity_pct
# MAGIC FROM insurance_catalog.silver.silver_property_policies
# MAGIC UNION ALL
# MAGIC SELECT 'silver_auto_policies', COUNT(*),
# MAGIC   SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END),
# MAGIC   ROUND(SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
# MAGIC FROM insurance_catalog.silver.silver_auto_policies
# MAGIC UNION ALL
# MAGIC SELECT 'silver_workers_comp_policies', COUNT(*),
# MAGIC   SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END),
# MAGIC   ROUND(SUM(CASE WHEN _is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
# MAGIC FROM insurance_catalog.silver.silver_workers_comp_policies
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer — Dimension and Fact Counts
# MAGIC
# MAGIC **TRAINEE NOTE — Expected row counts:**
# MAGIC - `dim_date`: ~23,741 rows (every date from 1970-01-01 to 2035-12-31)
# MAGIC - `dim_line_of_business`: 5 rows (PROP, WC, AUTO, GL, UMB — static reference data)
# MAGIC - `dim_insured`, `dim_agent`, `dim_location`: One row per unique entity in Silver
# MAGIC - `dim_policy`: One row per unique policy_id across all LOBs
# MAGIC - `dim_claim`: One row per unique claim_id across all LOBs
# MAGIC - `fact_premium`: One row per policy-coverage combination (>= dim_policy count)
# MAGIC - `fact_claim_transaction`: One row per claim payment/reserve transaction
# MAGIC - `mart_policy_360`: Should equal dim_policy count (grain = 1 policy)
# MAGIC
# MAGIC **Red flags:**
# MAGIC - dim_policy count ≠ mart_policy_360 count → mart build issue
# MAGIC - Any dim with 0 rows → Silver step failed for that LOB
# MAGIC - fact_premium = 0 → No Silver policy data was found when building the fact

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All Gold tables in one summary view.
# MAGIC -- Includes both dimensions and facts for a complete picture.
# MAGIC SELECT 'dim_date' AS dimension, COUNT(*) AS row_count FROM insurance_catalog.gold.dim_date
# MAGIC UNION ALL SELECT 'dim_line_of_business', COUNT(*) FROM insurance_catalog.gold.dim_line_of_business
# MAGIC UNION ALL SELECT 'dim_insured', COUNT(*) FROM insurance_catalog.gold.dim_insured
# MAGIC UNION ALL SELECT 'dim_location', COUNT(*) FROM insurance_catalog.gold.dim_location
# MAGIC UNION ALL SELECT 'dim_agent', COUNT(*) FROM insurance_catalog.gold.dim_agent
# MAGIC UNION ALL SELECT 'dim_coverage', COUNT(*) FROM insurance_catalog.gold.dim_coverage
# MAGIC UNION ALL SELECT 'dim_policy', COUNT(*) FROM insurance_catalog.gold.dim_policy
# MAGIC UNION ALL SELECT 'dim_claim', COUNT(*) FROM insurance_catalog.gold.dim_claim
# MAGIC UNION ALL SELECT 'fact_premium', COUNT(*) FROM insurance_catalog.gold.fact_premium
# MAGIC UNION ALL SELECT 'fact_claim_transaction', COUNT(*) FROM insurance_catalog.gold.fact_claim_transaction
# MAGIC UNION ALL SELECT 'fact_policy_transaction', COUNT(*) FROM insurance_catalog.gold.fact_policy_transaction
# MAGIC UNION ALL SELECT 'mart_policy_360', COUNT(*) FROM insurance_catalog.gold.mart_policy_360
# MAGIC ORDER BY dimension;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premium Distribution by LOB
# MAGIC
# MAGIC **TRAINEE NOTE — What this query shows:**
# MAGIC This joins the fact_premium table (measures) with dim_line_of_business
# MAGIC (context) to show how revenue breaks down by insurance line.
# MAGIC
# MAGIC In a real insurer's portfolio you'd typically see:
# MAGIC - Property (PROP) as the largest premium volume for homeowners/commercial
# MAGIC - Auto (AUTO) as high volume due to personal lines
# MAGIC - General Liability (GL) with high average premiums (large commercial risks)
# MAGIC - Workers' Comp (WC) tied to employer payroll size
# MAGIC - Umbrella (UMB) as the smallest (it's excess coverage, lower premium)
# MAGIC
# MAGIC **Key metrics:**
# MAGIC - `total_written_premium`: Gross revenue before reinsurance
# MAGIC - `avg_premium`: Average premium per policy-coverage row
# MAGIC - `total_net_premium`: What the insurer keeps after reinsurance (85% of written)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Premium analysis: JOIN fact with dimension to get human-readable LOB names.
# MAGIC -- fp = fact_premium alias, lob = dim_line_of_business alias.
# MAGIC -- This is the star schema pattern in action: fact JOIN dim ON surrogate key.
# MAGIC -- ROUND(..., 2) formats dollar amounts to 2 decimal places.
# MAGIC SELECT
# MAGIC   lob.lob_name,
# MAGIC   COUNT(*) AS policy_count,
# MAGIC   ROUND(SUM(fp.written_premium), 2) AS total_written_premium,
# MAGIC   ROUND(AVG(fp.written_premium), 2) AS avg_premium,
# MAGIC   ROUND(SUM(fp.net_premium), 2) AS total_net_premium
# MAGIC FROM insurance_catalog.gold.fact_premium fp
# MAGIC JOIN insurance_catalog.gold.dim_line_of_business lob ON fp.lob_key = lob.lob_key
# MAGIC GROUP BY lob.lob_name
# MAGIC ORDER BY total_written_premium DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims Summary by LOB
# MAGIC
# MAGIC **TRAINEE NOTE — Loss Ratio interpretation:**
# MAGIC This query reads directly from mart_policy_360 (no joins needed — it's already
# MAGIC denormalized). The mart pre-computes total_claims_count and loss_ratio per policy,
# MAGIC so we just aggregate at the LOB level.
# MAGIC
# MAGIC `avg_loss_ratio` is the KEY performance metric here:
# MAGIC - Loss Ratio = Total Incurred Claims / Written Premium
# MAGIC - Below 0.60: Underwriting profit (good for insurer)
# MAGIC - 0.60-0.70: Target range for most P&C lines
# MAGIC - Above 1.00: Underwriting loss (claims > premium — needs rate action)
# MAGIC
# MAGIC `WHERE total_claims_count > 0` filters to only policies that have had at
# MAGIC least one claim. Policies without claims have loss_ratio = 0 and would
# MAGIC skew the average downward, obscuring the true claims experience.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Claims summary by LOB from the data mart.
# MAGIC -- The mart pre-aggregates claims to policy level, so we re-aggregate
# MAGIC -- to LOB level here. This two-level aggregation is common in BI queries.
# MAGIC -- AVG(loss_ratio) gives the average loss ratio across policies WITH claims.
# MAGIC SELECT
# MAGIC   m.lob_name,
# MAGIC   COUNT(*) AS policy_count,
# MAGIC   SUM(m.total_claims_count) AS total_claims,
# MAGIC   ROUND(AVG(m.loss_ratio), 4) AS avg_loss_ratio,
# MAGIC   ROUND(SUM(m.total_paid_amount), 2) AS total_paid
# MAGIC FROM insurance_catalog.gold.mart_policy_360 m
# MAGIC WHERE m.total_claims_count > 0  -- Only policies that have had at least one claim
# MAGIC GROUP BY m.lob_name
# MAGIC ORDER BY total_claims DESC;
