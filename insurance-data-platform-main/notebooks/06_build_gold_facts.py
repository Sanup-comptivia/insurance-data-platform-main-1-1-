# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Build All Fact Tables
# MAGIC Builds 3 fact tables: fact_premium, fact_claim_transaction, fact_policy_transaction.
# MAGIC
# MAGIC **TRAINEE GUIDE — What are Gold Fact tables?**
# MAGIC Fact tables store the MEASURABLE business events — the transactions,
# MAGIC amounts, and counts that power analytics and reporting.
# MAGIC
# MAGIC Unlike dimension tables (which describe entities), fact tables record
# MAGIC what HAPPENED and HOW MUCH. Each row is one measurable event at a
# MAGIC specific grain (level of detail).
# MAGIC
# MAGIC **Three fact tables in this platform:**
# MAGIC
# MAGIC | Fact Table | Grain | Key Measures |
# MAGIC |------------|-------|--------------|
# MAGIC | fact_premium | 1 row per policy-coverage record | written_premium, earned, ceded, net, commission |
# MAGIC | fact_claim_transaction | 1 row per claim payment/reserve transaction | paid_amount, reserve_amount, total_incurred |
# MAGIC | fact_policy_transaction | 1 row per policy lifecycle event | transaction_amount, change_type |
# MAGIC
# MAGIC **Why do facts need dimensions to be built first?**
# MAGIC Fact tables contain ONLY surrogate keys (FKs) to dimensions — they don't
# MAGIC store the actual names or descriptions. The build process reads from the
# MAGIC already-built dimension tables to RESOLVE natural keys into surrogate keys:
# MAGIC   - policy_id → policy_key (from dim_policy)
# MAGIC   - coverage_id → coverage_key (from dim_coverage)
# MAGIC   - effective_date → date_key (YYYYMMDD integer)
# MAGIC
# MAGIC **Run AFTER:** `05_build_gold_dimensions.py`
# MAGIC **Run BEFORE:** `07_build_mart.py`

# COMMAND ----------

import os
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

print("=" * 60)
print("Gold Layer — Building Fact Tables")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_premium
# MAGIC
# MAGIC **TRAINEE NOTE — Premium fact table deep dive:**
# MAGIC This is the most important financial fact table. Each row represents
# MAGIC the premium for one coverage type on one policy.
# MAGIC
# MAGIC **Why "written" vs "earned" premium?**
# MAGIC - **Written premium**: The full annual premium billed when the policy starts.
# MAGIC   This is recognized as revenue on the WRITTEN date.
# MAGIC - **Earned premium**: Premium "earned" as each day of coverage passes.
# MAGIC   After 6 months of a 12-month policy, 50% of premium is "earned".
# MAGIC   In this platform, earned = written (simplified).
# MAGIC
# MAGIC **Why cede premium to reinsurers?**
# MAGIC Reinsurance is "insurance for insurance companies". When an insurer faces
# MAGIC very large potential losses (e.g. a hurricane wiping out thousands of
# MAGIC properties), it buys reinsurance to cap its exposure. The cost is the
# MAGIC "ceded premium" (15% in this platform). In exchange, the reinsurer covers
# MAGIC losses above a threshold.
# MAGIC
# MAGIC **Key measures per row:**
# MAGIC - `written_premium`: What was billed to the insured
# MAGIC - `ceded_premium`: 15% passed to reinsurer
# MAGIC - `net_premium`: 85% retained by the insurer
# MAGIC - `commission_amount`: 12% paid to the selling agent
# MAGIC - `tax_amount`: 3% state premium tax

# COMMAND ----------

from src.gold.facts.fact_premium import build_fact_premium
build_fact_premium(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_claim_transaction
# MAGIC
# MAGIC **TRAINEE NOTE — Claim transaction fact table:**
# MAGIC Insurance claims go through a lifecycle that involves multiple financial
# MAGIC transactions over time. fact_claim_transaction captures each transaction.
# MAGIC
# MAGIC **Claim lifecycle example:**
# MAGIC 1. Claim filed → insurer sets an INITIAL RESERVE (estimated future payments)
# MAGIC 2. Medical bills arrive → PAYMENT made, reserve adjusted
# MAGIC 3. Ongoing treatment → additional PAYMENTS, reserve updated
# MAGIC 4. Claim settled → final PAYMENT, reserve set to zero, claim CLOSED
# MAGIC
# MAGIC Each step above generates one row in fact_claim_transaction.
# MAGIC
# MAGIC **Key measures:**
# MAGIC - `paid_amount`: Cash payment made in this transaction
# MAGIC - `reserve_amount`: Current estimate of future payments remaining
# MAGIC - `total_incurred`: paid_amount + reserve_amount = total expected cost
# MAGIC   (the most important measure — used to compute loss ratio)
# MAGIC
# MAGIC **Key FKs:**
# MAGIC - `claim_key` → dim_claim
# MAGIC - `policy_key` → dim_policy (shortcut for policy-level aggregation)
# MAGIC - `date_key` → dim_date (when this transaction occurred)
# MAGIC - `lob_key` → dim_line_of_business (which LOB this claim belongs to)

# COMMAND ----------

from src.gold.facts.fact_claim_transaction import build_fact_claim_transaction
build_fact_claim_transaction(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_policy_transaction
# MAGIC
# MAGIC **TRAINEE NOTE — Policy transaction fact table:**
# MAGIC Beyond premiums and claims, policies have their own lifecycle events:
# MAGIC - New business (policy first written)
# MAGIC - Renewal (policy renewed for another term)
# MAGIC - Endorsement (mid-term change to coverage or premium)
# MAGIC - Cancellation (policy terminated early)
# MAGIC - Reinstatement (policy restored after lapse)
# MAGIC
# MAGIC Each event is a "policy transaction" with a type, effective date, and
# MAGIC potentially a premium adjustment.
# MAGIC
# MAGIC This fact table is useful for:
# MAGIC - Tracking portfolio retention (how many renew vs. cancel?)
# MAGIC - Understanding mid-term endorsement patterns
# MAGIC - Computing earned premium more precisely (accounting for cancellations)
# MAGIC
# MAGIC **Key measures:**
# MAGIC - `transaction_amount`: Premium change associated with this event
# MAGIC - `transaction_type`: New Business / Renewal / Endorsement / Cancellation

# COMMAND ----------

from src.gold.facts.fact_policy_transaction import build_fact_policy_transaction
build_fact_policy_transaction(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Facts
# MAGIC
# MAGIC **TRAINEE NOTE — What row counts to expect:**
# MAGIC - `fact_premium`: Should have >= dim_policy count rows
# MAGIC   (one row per policy per coverage type; most policies have 1 coverage)
# MAGIC - `fact_claim_transaction`: Should have >= dim_claim count rows
# MAGIC   (one or more transactions per claim depending on claim lifecycle)
# MAGIC - `fact_policy_transaction`: Should have >= dim_policy count rows
# MAGIC   (at minimum one "New Business" transaction per policy)
# MAGIC
# MAGIC **Red flags:**
# MAGIC - Any fact = 0 rows → Silver data was missing when the fact was built
# MAGIC - fact_premium << dim_policy → dim_policy was empty when fact was built
# MAGIC   (rebuild fact_premium after confirming dim_policy has rows)

# COMMAND ----------

facts = ["fact_premium", "fact_claim_transaction", "fact_policy_transaction"]
print("\nGold Fact Summary:")
for f in facts:
    try:
        count = spark.table(f"insurance_catalog.gold.{f}").count()
        print(f"  {f}: {count:,} rows")
    except Exception as e:
        print(f"  {f}: ERROR — {e}")

print("\nAll fact tables built!")
print("Next step: Run 07_build_mart.py")
