# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Build All Dimension Tables
# MAGIC Builds 8 dimension tables in dependency order.
# MAGIC
# MAGIC **TRAINEE GUIDE — What are Gold dimension tables?**
# MAGIC Dimension tables are the descriptive "context" tables in a Star Schema.
# MAGIC They answer WHO, WHAT, WHERE, and WHEN about business events:
# MAGIC
# MAGIC | Dimension | Describes | Grain |
# MAGIC |-----------|-----------|-------|
# MAGIC | dim_date | Every calendar date | 1 row per calendar day |
# MAGIC | dim_line_of_business | Insurance LOBs | 1 row per LOB |
# MAGIC | dim_insured | People/businesses insured | 1 row per insured |
# MAGIC | dim_location | Geographic locations of risks | 1 row per location |
# MAGIC | dim_agent | Insurance agents | 1 row per agent |
# MAGIC | dim_coverage | Coverage types | 1 row per coverage type |
# MAGIC | dim_policy | Insurance policies | 1 row per policy_id |
# MAGIC | dim_claim | Insurance claims | 1 row per claim_id |
# MAGIC
# MAGIC **Dimension build dependency order (CRITICAL):**
# MAGIC Some dimensions join to other dimensions to get surrogate keys (FKs).
# MAGIC They must be built in the correct order:
# MAGIC ```
# MAGIC [No dependencies]             → dim_date, dim_line_of_business,
# MAGIC                                  dim_insured, dim_location, dim_agent, dim_coverage
# MAGIC [Depends on 1-4 above]        → dim_policy
# MAGIC [Depends on dim_policy]       → dim_claim
# MAGIC ```
# MAGIC
# MAGIC **Run AFTER:** `04_run_silver.py` (Silver tables must exist as input)
# MAGIC **Run BEFORE:** `06_build_gold_facts.py` (facts need dimension keys)

# COMMAND ----------

import os
import sys
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(f"/Workspace{notebook_path}"))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

print("=" * 60)
print("Gold Layer — Building Dimension Tables")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_date (no dependencies)
# MAGIC
# MAGIC **TRAINEE NOTE — Why do we need a date dimension?**
# MAGIC dim_date is a calendar lookup table containing every date from 1970 to 2035.
# MAGIC Each row represents one calendar day and includes derived attributes:
# MAGIC - `date_key`: Integer YYYYMMDD (e.g. 20240115) — the PK used in facts
# MAGIC - `full_date`: Actual DateType value
# MAGIC - `year`, `quarter`, `month`, `week`, `day_of_week`: For time-series analysis
# MAGIC - `is_weekend`, `is_holiday`: For business day calculations
# MAGIC - `month_name`, `day_name`: Human-readable labels
# MAGIC
# MAGIC Why pre-compute all these? So that analytical queries don't need to call
# MAGIC date functions (YEAR(), MONTH(), etc.) at query time — they just JOIN to
# MAGIC dim_date and filter on pre-computed attributes. This is much faster.
# MAGIC
# MAGIC dim_date is completely independent — it is generated from scratch using
# MAGIC a date sequence, with no dependency on insurance source data.

# COMMAND ----------

from src.gold.dimensions.dim_date import build_dim_date
build_dim_date(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_line_of_business (no dependencies)
# MAGIC
# MAGIC **TRAINEE NOTE — Why a dimension for LOB?**
# MAGIC Line of Business is static reference data — there are exactly 5 LOBs in
# MAGIC this platform. Rather than storing "Property" as a string in every fact row
# MAGIC (which wastes storage and creates inconsistency risk), we store a compact
# MAGIC surrogate key (lob_key) and join to dim_line_of_business for the full name.
# MAGIC
# MAGIC dim_line_of_business includes:
# MAGIC - `lob_key`: Surrogate PK (MD5 hash of lob_code)
# MAGIC - `lob_code`: Short code (PROP, WC, AUTO, GL, UMB)
# MAGIC - `lob_name`: Full name (Property, Workers' Compensation, Auto, etc.)
# MAGIC - `lob_category`: Property vs. Casualty (regulatory/accounting classification)
# MAGIC - `naic_code`: NAIC code for regulatory filings
# MAGIC
# MAGIC This dimension is STATIC — it doesn't change unless the company enters
# MAGIC a new line of business. It is populated from hard-coded values, not from
# MAGIC Silver tables.

# COMMAND ----------

from src.gold.dimensions.dim_line_of_business import build_dim_line_of_business
build_dim_line_of_business(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_insured (depends on Silver)
# MAGIC
# MAGIC **TRAINEE NOTE — What is the Insured dimension?**
# MAGIC The insured is the person or business that holds the insurance policy.
# MAGIC One insured can have MULTIPLE policies (e.g. same person has auto + umbrella).
# MAGIC
# MAGIC dim_insured consolidates insured data across all 5 Silver LOB policy tables.
# MAGIC Each unique insured_id gets one row with:
# MAGIC - `insured_key`: Surrogate PK
# MAGIC - `insured_id`: Natural key from source (policy_id used as proxy here)
# MAGIC - `insured_name`: Full name (individual) or company name (business)
# MAGIC - `insured_type`: Individual or Business
# MAGIC - `date_of_birth`: For individuals (null for businesses)
# MAGIC - `ein`: Employer Identification Number (for businesses)
# MAGIC - `naics_code`, `industry`: Business classification
# MAGIC - `annual_revenue`, `employee_count`: Business size metrics

# COMMAND ----------

from src.gold.dimensions.dim_insured import build_dim_insured
build_dim_insured(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_location (depends on Silver)
# MAGIC
# MAGIC **TRAINEE NOTE — What is the Location dimension?**
# MAGIC dim_location stores the geographic location of the insured RISK (not the
# MAGIC insured's home address — these can differ for commercial properties).
# MAGIC
# MAGIC Location is important in insurance because:
# MAGIC - Geographic risk concentration affects pricing (catastrophe models)
# MAGIC - State-specific regulations apply (different by state)
# MAGIC - Flood zones, wind corridors, crime rates vary by location
# MAGIC
# MAGIC Key columns:
# MAGIC - `location_key`: Surrogate PK
# MAGIC - `address_line1`, `city`, `state_code`, `zip_code`: Address fields
# MAGIC - `latitude`, `longitude`: GPS coordinates (for geographic analysis)
# MAGIC - `flood_zone`: FEMA flood zone (A/AE/X/etc.) — critical for property rating
# MAGIC - `county`, `state_name`: Enriched from state_code lookup

# COMMAND ----------

from src.gold.dimensions.dim_location import build_dim_location
build_dim_location(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_agent (depends on Silver)
# MAGIC
# MAGIC **TRAINEE NOTE — What is the Agent dimension?**
# MAGIC Insurance agents are the salespeople who sell policies to insureds.
# MAGIC They earn commission (typically 10-15% of written premium).
# MAGIC
# MAGIC dim_agent enables agent performance analysis:
# MAGIC - Which agents produce the most premium?
# MAGIC - Which agents have the worst loss ratios (are they writing risky business)?
# MAGIC - Which agencies have the highest commission tier?
# MAGIC
# MAGIC Key columns:
# MAGIC - `agent_key`: Surrogate PK
# MAGIC - `agent_id`, `agent_name`: Agent identification
# MAGIC - `agency_name`, `agency_code`: The agency (firm) the agent works for
# MAGIC - `license_number`, `license_state`: Regulatory license info
# MAGIC - `commission_tier`: Gold/Silver/Standard — determines commission rate

# COMMAND ----------

from src.gold.dimensions.dim_agent import build_dim_agent
build_dim_agent(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. dim_coverage (depends on dim_line_of_business)
# MAGIC
# MAGIC **TRAINEE NOTE — What is the Coverage dimension?**
# MAGIC Coverage types describe WHAT is covered under a policy. One policy can
# MAGIC have multiple coverage types (e.g. a property policy might cover both
# MAGIC building AND contents separately).
# MAGIC
# MAGIC Coverage codes used in this platform:
# MAGIC - `COV-PROP-BLDG`: Property building coverage
# MAGIC - `COV-WC-MED`: Workers' Comp medical coverage
# MAGIC - `COV-AUTO-BI`: Auto bodily injury liability
# MAGIC - `COV-GL-PREM`: GL premises liability
# MAGIC - `COV-UMB-EXCESS`: Umbrella excess liability
# MAGIC
# MAGIC dim_coverage depends on dim_line_of_business because it needs to store
# MAGIC lob_key as a FK (each coverage type belongs to a specific LOB).

# COMMAND ----------

from src.gold.dimensions.dim_coverage import build_dim_coverage
build_dim_coverage(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. dim_policy (depends on dim_lob, dim_insured, dim_agent, dim_location)
# MAGIC
# MAGIC **TRAINEE NOTE — Why is dim_policy built last among the "core" dims?**
# MAGIC dim_policy is the CENTRAL hub of the star schema. It joins to:
# MAGIC - dim_line_of_business (to get lob_key)
# MAGIC - dim_insured (to get insured_key)
# MAGIC - dim_agent (to get agent_key)
# MAGIC - dim_location (for location_key — placeholder for now)
# MAGIC - dim_date (to convert dates to date_key integers)
# MAGIC
# MAGIC Because dim_policy READS from all these other dimensions to resolve
# MAGIC surrogate keys, all 4 must exist BEFORE dim_policy is built.
# MAGIC
# MAGIC dim_policy consolidates ALL 5 LOBs into a single table of policies.
# MAGIC One row per unique policy_id across ALL lines of business.

# COMMAND ----------

from src.gold.dimensions.dim_policy import build_dim_policy
build_dim_policy(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. dim_claim (depends on dim_policy)
# MAGIC
# MAGIC **TRAINEE NOTE — What is the Claim dimension?**
# MAGIC dim_claim stores one row per unique insurance claim across all 5 LOBs.
# MAGIC It is a dimension (not a fact) because it stores descriptive attributes
# MAGIC of the claim entity (who, when, what caused it, current status).
# MAGIC
# MAGIC The financial amounts (how much was paid, how much is reserved) live in
# MAGIC fact_claim_transaction — the fact table. This separation follows
# MAGIC the Kimball data warehouse principle: facts hold measurements, dimensions
# MAGIC hold descriptive context.
# MAGIC
# MAGIC Key columns:
# MAGIC - `claim_key`: Surrogate PK
# MAGIC - `claim_id`: Natural key from source
# MAGIC - `policy_key`: FK → dim_policy (which policy this claim is against)
# MAGIC - `date_of_loss`: When the loss event occurred
# MAGIC - `cause_of_loss`: e.g. "flood", "accident", "fire", "injury"
# MAGIC - `claim_status`: Open / Closed / Pending
# MAGIC
# MAGIC dim_claim depends on dim_policy because every claim must reference
# MAGIC a valid policy (a claim without a policy is an orphan record — invalid).

# COMMAND ----------

from src.gold.dimensions.dim_claim import build_dim_claim
build_dim_claim(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Dimensions
# MAGIC
# MAGIC **TRAINEE NOTE — Expected row counts:**
# MAGIC - `dim_date`: ~23,741 rows (1970-01-01 to 2035-12-31)
# MAGIC - `dim_line_of_business`: 5 rows (one per LOB — static)
# MAGIC - Other dims: proportional to how many policies/claims were generated
# MAGIC   (local mode ~100K policies → ~100K rows in dim_policy and dim_insured)
# MAGIC
# MAGIC **If a dimension shows ERROR:**
# MAGIC - Check the corresponding Silver table exists and has rows
# MAGIC - Check the build function output for WARN messages about missing Silver data
# MAGIC - Re-run just that dimension after fixing the upstream issue

# COMMAND ----------

# Loop over all 8 dimension names and print row counts.
# try/except per dimension so a single failure doesn't hide the others.
dims = ["dim_date", "dim_line_of_business", "dim_insured", "dim_location",
        "dim_agent", "dim_coverage", "dim_policy", "dim_claim"]
print("\nGold Dimension Summary:")
for d in dims:
    try:
        count = spark.table(f"insurance_catalog.gold.{d}").count()
        print(f"  {d}: {count:,} rows")
    except Exception as e:
        print(f"  {d}: ERROR — {e}")

print("\nAll dimensions built!")
print("Next step: Run 06_build_gold_facts.py")
