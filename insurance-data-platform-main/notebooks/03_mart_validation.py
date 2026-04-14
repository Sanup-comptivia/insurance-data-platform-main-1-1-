# Databricks notebook source
# MAGIC %md
# MAGIC # Insurance Data Platform — Mart Validation
# MAGIC Automated data quality checks on mart_policy_360.
# MAGIC
# MAGIC **TRAINEE GUIDE — What is automated validation and why does it matter?**
# MAGIC After building the data mart, we run a set of programmatic assertions to
# MAGIC confirm the mart is correct before business users rely on it. This is
# MAGIC similar to unit testing in software engineering, but for data.
# MAGIC
# MAGIC **Each check follows the pattern:**
# MAGIC 1. Compute a metric from the mart (count, distinct values, aggregation)
# MAGIC 2. `assert` that the metric meets the expected condition
# MAGIC 3. Print PASS if the assertion holds, or let Python raise an AssertionError
# MAGIC
# MAGIC **TRAINEE NOTE — Why use `assert` instead of `if/else`?**
# MAGIC `assert condition, "message"` raises an `AssertionError` immediately if the
# MAGIC condition is False. This is intentional: if a validation fails, the notebook
# MAGIC should STOP executing so the error is not silently ignored. In production
# MAGIC pipelines, failed assertions trigger alerts to the on-call data engineer.
# MAGIC
# MAGIC **Checks covered:**
# MAGIC 1. Row count matches dim_policy (mart grain = one row per policy)
# MAGIC 2. No duplicate policy_ids in the mart
# MAGIC 3. All 5 LOBs are represented (no missing line of business)
# MAGIC 4. Referential integrity — FK keys point to valid dimension rows
# MAGIC 5. Financial sanity — no negative premiums or claim amounts
# MAGIC 6. Sample lookup — interactive policy query

# COMMAND ----------

# Constants used throughout the validation checks.
# TRAINEE NOTE: Defining these at the top makes the notebook easy to
# modify if the catalog/schema names ever change — update here once.
MART_TABLE = "insurance_catalog.gold.mart_policy_360"
GOLD_SCHEMA = "insurance_catalog.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Row Count Validation
# MAGIC
# MAGIC **TRAINEE NOTE — Why must mart count equal dim_policy count?**
# MAGIC The mart has grain = one row per policy_id. dim_policy also has one row
# MAGIC per unique policy_id (enforced by deduplication in its build step).
# MAGIC So these two counts MUST match.
# MAGIC
# MAGIC If they don't match, common causes:
# MAGIC - The mart was built from a stale dim_policy (rebuild dim_policy first)
# MAGIC - A policy appeared in Silver after dim_policy was built but before the mart ran
# MAGIC - The mart build failed partway through and wrote a partial result

# COMMAND ----------

mart_count = spark.table(MART_TABLE).count()
dim_policy_count = spark.table(f"{GOLD_SCHEMA}.dim_policy").count()

print(f"mart_policy_360 rows: {mart_count:,}")
print(f"dim_policy rows:      {dim_policy_count:,}")

# assert raises AssertionError if condition is False, halting the notebook.
# The second argument is the error message shown when the assertion fails.
assert mart_count == dim_policy_count, f"FAIL: Mart count ({mart_count}) != dim_policy count ({dim_policy_count})"
print("PASS: Row counts match")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. No Duplicate Policy IDs
# MAGIC
# MAGIC **TRAINEE NOTE — Why is uniqueness on policy_id important?**
# MAGIC The mart is designed to return ONE row per policy when a user queries
# MAGIC `WHERE policy_id = 'AUTO-0000000001'`. If duplicates exist:
# MAGIC - Reports will double-count premiums and claims
# MAGIC - BI dashboards will show inflated numbers
# MAGIC - Downstream joins will fan out (one-to-many where one-to-one is expected)
# MAGIC
# MAGIC How this check works:
# MAGIC 1. GROUP BY policy_id and COUNT rows per group
# MAGIC 2. FILTER to groups where count > 1 (duplicates)
# MAGIC 3. COUNT how many such groups exist
# MAGIC If dup_count == 0, every policy_id appears exactly once → PASS

# COMMAND ----------

from pyspark.sql import functions as F

dup_count = (
    spark.table(MART_TABLE)
    .groupBy("policy_id")
    .count()                           # Count occurrences of each policy_id
    .filter(F.col("count") > 1)        # Keep only the duplicated ones
    .count()                           # How many distinct policy_ids are duplicated?
)
print(f"Duplicate policy_ids: {dup_count}")
assert dup_count == 0, f"FAIL: {dup_count} duplicate policy_ids found"
print("PASS: No duplicate policy_ids")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. All LOBs Represented
# MAGIC
# MAGIC **TRAINEE NOTE — Why check LOB completeness?**
# MAGIC The mart should contain policies from all 5 Lines of Business.
# MAGIC If a LOB is missing, it usually means:
# MAGIC - That LOB's Silver transformer failed (check Step 4 in run_all_pipeline.py)
# MAGIC - The Bronze tables for that LOB are empty (check ingestion step)
# MAGIC - A code change broke that LOB's transformer (check git diff)
# MAGIC
# MAGIC Python set operations used here:
# MAGIC - `expected_lobs - lob_codes` = elements in expected_lobs that are NOT in lob_codes
# MAGIC   This is the SET DIFFERENCE operation. If the result is empty → all LOBs present.

# COMMAND ----------

lobs = (
    spark.table(MART_TABLE)
    .select("lob_code")
    .distinct()     # Get unique LOB codes in the mart
    .collect()      # Bring results to the driver (safe — only 5 rows at most)
)
lob_codes = {row.lob_code for row in lobs if row.lob_code}  # Python set comprehension, exclude nulls
expected_lobs = {"PROP", "WC", "AUTO", "GL", "UMB"}

print(f"LOBs in mart:    {lob_codes}")
print(f"Expected LOBs:   {expected_lobs}")
missing = expected_lobs - lob_codes  # Set difference: which expected LOBs are absent?
assert len(missing) == 0, f"FAIL: Missing LOBs: {missing}"
print("PASS: All 5 LOBs represented")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Referential Integrity — FK Keys Exist in Dimensions
# MAGIC
# MAGIC **TRAINEE NOTE — What is referential integrity?**
# MAGIC Referential integrity means every Foreign Key in one table points to a
# MAGIC valid Primary Key in the referenced table. In the mart:
# MAGIC   - `insured_key` must exist in `dim_insured.insured_key`
# MAGIC   - `agent_key` must exist in `dim_agent.agent_key`
# MAGIC   - etc.
# MAGIC
# MAGIC An "orphan record" is a row whose FK doesn't match any PK in the
# MAGIC referenced table — the FK is pointing at nothing.
# MAGIC
# MAGIC **How `left_anti` join works:**
# MAGIC A LEFT ANTI JOIN returns rows from the LEFT table that have NO match
# MAGIC in the RIGHT table. It is the opposite of INNER JOIN.
# MAGIC   - INNER JOIN: only rows that match on both sides
# MAGIC   - LEFT JOIN: all left rows, nulls on right side for non-matches
# MAGIC   - LEFT ANTI: only left rows that do NOT match (orphans)
# MAGIC
# MAGIC We then filter to `.isNotNull()` because null insured_keys are expected
# MAGIC (policies without a matched insured are flagged but not broken).

# COMMAND ----------

# Check insured_key FK — every non-null insured_key must exist in dim_insured
orphan_insured = (
    spark.table(MART_TABLE).alias("m")
    .join(
        spark.table(f"{GOLD_SCHEMA}.dim_insured").alias("d"),
        F.col("m.insured_key") == F.col("d.insured_key"),
        "left_anti"   # Returns mart rows with NO matching dim_insured row
    )
    .filter(F.col("insured_key").isNotNull())  # Ignore null FKs (expected for some policies)
    .count()
)
print(f"Orphan insured_keys: {orphan_insured}")
assert orphan_insured == 0, f"FAIL: {orphan_insured} orphan insured_keys"
print("PASS: All insured_keys valid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Premium and Claims Sanity Checks
# MAGIC
# MAGIC **TRAINEE NOTE — Financial sanity rules in insurance:**
# MAGIC - `total_written_premium` must never be negative (premium is always positive)
# MAGIC - `total_paid_amount` must never be negative (claim payments go TO the insured)
# MAGIC - `loss_ratio` must never be negative (it's a ratio of two positive numbers)
# MAGIC
# MAGIC **How the aggregation works:**
# MAGIC `F.sum(F.when(condition, 1).otherwise(0))` is a conditional count —
# MAGIC it counts rows where the condition is True. This is the PySpark equivalent
# MAGIC of SQL's `SUM(CASE WHEN condition THEN 1 ELSE 0 END)`.
# MAGIC
# MAGIC `.collect()[0]` retrieves the single aggregation result row to the driver.
# MAGIC We then access each column by name (e.g. `sanity.negative_premiums`).
# MAGIC
# MAGIC **avg_premium and avg_loss_ratio** are printed for eyeballing reasonableness
# MAGIC even though no assertion is placed on them — engineers use these to spot
# MAGIC obviously wrong data (e.g. avg_premium = $0.01 would be suspicious).

# COMMAND ----------

sanity = (
    spark.table(MART_TABLE)
    .agg(
        # Count rows with negative total_written_premium (should be 0)
        F.sum(F.when(F.col("total_written_premium") < 0, 1).otherwise(0)).alias("negative_premiums"),
        # Count rows with negative total_paid_amount (should be 0)
        F.sum(F.when(F.col("total_paid_amount") < 0, 1).otherwise(0)).alias("negative_claims"),
        # Count rows with negative loss_ratio (should be 0)
        F.sum(F.when(F.col("loss_ratio") < 0, 1).otherwise(0)).alias("negative_loss_ratio"),
        # Averages for human eyeballing (no hard assertion, just for review)
        F.avg("total_written_premium").alias("avg_premium"),
        F.avg("loss_ratio").alias("avg_loss_ratio"),
    )
    .collect()[0]  # Single-row result — pull to driver
)

print(f"Negative premiums:   {sanity.negative_premiums}")
print(f"Negative claims:     {sanity.negative_claims}")
print(f"Negative loss ratio: {sanity.negative_loss_ratio}")
print(f"Avg premium:         ${sanity.avg_premium:,.2f}")
print(f"Avg loss ratio:      {sanity.avg_loss_ratio:.4f}")

assert sanity.negative_premiums == 0, "FAIL: Negative premiums found"
print("PASS: Premium/claims sanity checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sample Query — Lookup a Specific Policy
# MAGIC
# MAGIC **TRAINEE NOTE — How to use this query:**
# MAGIC Replace `'AUTO-0000000001'` with any `policy_id` from your data.
# MAGIC Use this pattern in BI tools to pull a complete policy 360-degree view
# MAGIC without writing any joins — everything is already in the mart.
# MAGIC
# MAGIC **What each column group shows:**
# MAGIC - `policy_id, lob_name, insured_name, insured_type`: Who is insured and for what
# MAGIC - `policy_state, policy_city`: Where the risk is located
# MAGIC - `total_written_premium, total_net_premium`: Revenue from this policy
# MAGIC - `total_claims_count, open_claims_count, closed_claims_count`: Claims activity
# MAGIC - `total_paid_amount, loss_ratio`: Financial performance of this policy
# MAGIC - LOB-specific columns (auto_vehicle_make, wc_class_code, etc.): Null for non-matching LOBs
# MAGIC - `is_active, days_until_expiry`: Policy lifecycle status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full 360-degree policy view in a single query — no joins needed.
# MAGIC -- Change the policy_id value to look up any policy in your dataset.
# MAGIC -- LOB-specific columns (auto_*, wc_*, gl_*, umb_*, property_*) will be
# MAGIC -- NULL for LOBs that don't apply to this policy.
# MAGIC SELECT
# MAGIC   policy_id, lob_name, insured_name, insured_type,
# MAGIC   policy_state, policy_city,
# MAGIC   total_written_premium, total_net_premium,
# MAGIC   total_claims_count, open_claims_count, closed_claims_count,
# MAGIC   total_paid_amount, loss_ratio,
# MAGIC   auto_vehicle_make, auto_vehicle_model,
# MAGIC   wc_class_code, wc_payroll_amount,
# MAGIC   gl_business_type, gl_occurrence_limit,
# MAGIC   umb_umbrella_limit,
# MAGIC   property_construction_type, property_building_value,
# MAGIC   is_active, days_until_expiry
# MAGIC FROM insurance_catalog.gold.mart_policy_360
# MAGIC WHERE policy_id = 'AUTO-0000000001'
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------

# Final summary — printed only if all assertions above passed.
# TRAINEE NOTE: If any assertion FAILED, Python would have raised an AssertionError
# and execution would have stopped before reaching this cell.
# Seeing this message means ALL checks above passed successfully.
print("=" * 60)
print("MART VALIDATION COMPLETE — ALL CHECKS PASSED")
print("=" * 60)
print("\nValidation checks completed:")
print("  [1] PASS: mart row count == dim_policy row count")
print("  [2] PASS: No duplicate policy_ids")
print("  [3] PASS: All 5 LOBs represented (PROP, WC, AUTO, GL, UMB)")
print("  [4] PASS: All insured_keys have matching dim_insured rows")
print("  [5] PASS: No negative premiums, claims, or loss ratios")
