"""
Gold Layer — fact_premium fact table.
Grain: one row per policy-coverage premium record.
FK: policy_key, coverage_key, date_key, lob_key, location_key

TRAINEE GUIDE — What is a Fact Table?
In a Star Schema, fact tables store MEASURABLE business events or transactions.
Unlike dimension tables (which describe WHO/WHAT/WHERE), fact tables answer
HOW MUCH and HOW MANY:
  - How much written premium did we collect from policy PROP-001?
  - How much did we cede to reinsurers?
  - How much commission did the agent earn?

The GRAIN of a fact table is the level of detail — the lowest unit of
measurement that one row represents. Choosing the right grain is critical:

  fact_premium grain: ONE ROW per (policy, coverage type, effective date)
  Example: Policy PROP-001 with building coverage, effective 2024-01-01

Why not one row per policy?
  One policy can have multiple coverage types (building + contents + flood).
  Each coverage type has its own premium, limit, and deductible. Storing
  them as separate rows makes it easy to aggregate by coverage type or
  to join to dim_coverage for coverage-specific analysis.

Premium components explained (insurance terminology):
  written_premium  : The full annual premium billed to the insured.
                     This is the "top-line" revenue.
  earned_premium   : The portion of written_premium that has been "earned"
                     as coverage time passes. For a 1-year policy, after 6
                     months, earned_premium ≈ written_premium / 2. In this
                     simplified model, we set earned = written.
  ceded_premium    : The portion passed to a REINSURER. Reinsurance is
                     insurance for insurance companies — they pay a % of
                     premium (ceded) in exchange for the reinsurer covering
                     large losses. Here we use 15% as a typical placeholder.
  net_premium      : written_premium - ceded_premium. This is what the insurer
                     keeps after reinsurance costs.
  commission_amount: The fee paid to the agent for selling the policy (12%).
  tax_amount       : Premium taxes collected and remitted to state (3%).

Coverage ID codes used in this platform:
  COV-PROP-BLDG  : Property building coverage
  COV-WC-MED     : Workers' Comp medical coverage
  COV-AUTO-BI    : Auto bodily injury liability
  COV-GL-PREM    : General Liability premises coverage
  COV-UMB-EXCESS : Umbrella excess liability
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import generate_surrogate_key, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "fact_premium"


def build_fact_premium(spark: SparkSession):
    """Build premium fact table from Silver policy data across all LOBs.

    TRAINEE NOTE — Why do we read from Silver (not Bronze) for facts?
    Silver data is typed and cleansed — numeric columns are actual decimals,
    dates are DateType, states are uppercase. Fact tables need these proper
    types for correct aggregation (you can't SUM string columns).

    Joining to dimensions for FKs:
      Fact tables store FOREIGN KEYS (not the actual values). For example,
      instead of storing lob_name="Property", we store lob_key="a3f4b2c1..."
      This allows the fact table to be smaller (keys vs. full strings) and
      enables efficient joins to dimension tables in analytical queries.

      The pattern is:
        Load dim → select only the key + the natural key used for joining
        Join fact to dim ON natural key → get surrogate key
        Drop the natural key from the output (keep only the surrogate key)
    """
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    # Load dimension lookup tables for FK resolution.
    # We only select the columns we need for joining — avoids loading full dims.
    policy_df = _safe_table(
        spark, f"{CATALOG}.{GOLD}.dim_policy", ["policy_key", "policy_id", "lob_key", "location_key"]
    )
    coverage_df = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_coverage", ["coverage_key", "coverage_id", "lob_key"])

    frames = []  # One DataFrame per LOB, unioned at the end

    # ---- Property Premiums ----
    # TRAINEE NOTE: For each LOB, we select columns that map to the standard
    # premium fact schema. LOB-specific column names are aliased to standard names:
    #   total_building_coverage → coverage_limit (standard name)
    #   premium_amount          → written_premium (standard name)
    # We also assign a hardcoded coverage_id literal so each row can be
    # joined to dim_coverage to get the coverage_key FK.
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.col("policy_id"),
            F.lit("PROP").alias("lob_code"),                                   # Used to join dim_policy
            F.col("effective_date"),
            F.col("premium_amount").alias("written_premium"),                  # Rename to standard column
            F.col("total_building_coverage").alias("coverage_limit"),          # Building coverage limit
            F.col("deductible_amount"),
            F.lit("COV-PROP-BLDG").alias("coverage_id"),                      # Coverage type identifier
        )
        frames.append(prop)
    except Exception:
        pass

    # ---- Workers' Comp Premiums ----
    # WC uses payroll_amount as the "coverage limit" (exposure base for WC pricing)
    # WC has no traditional deductible → set to 0
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_policies").select(
            F.col("policy_id"),
            F.lit("WC").alias("lob_code"),
            F.col("effective_date"),
            F.col("premium_amount").alias("written_premium"),
            F.col("payroll_amount").alias("coverage_limit"),                   # WC exposure base = payroll
            F.lit(0).cast(DecimalType(18, 2)).alias("deductible_amount"),      # WC typically has no deductible
            F.lit("COV-WC-MED").alias("coverage_id"),
        )
        frames.append(wc)
    except Exception:
        pass

    # ---- Auto Premiums ----
    # Auto uses liability_limit as the coverage limit (max payout per accident)
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("policy_id"),
            F.lit("AUTO").alias("lob_code"),
            F.col("effective_date"),
            F.col("premium_amount").alias("written_premium"),
            F.col("liability_limit").alias("coverage_limit"),                  # Max payout per accident
            F.col("deductible_amount"),
            F.lit("COV-AUTO-BI").alias("coverage_id"),                        # Bodily injury coverage
        )
        frames.append(auto)
    except Exception:
        pass

    # ---- GL Premiums ----
    # GL uses occurrence_limit (max payout per occurrence/event)
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("policy_id"),
            F.lit("GL").alias("lob_code"),
            F.col("effective_date"),
            F.col("premium_amount").alias("written_premium"),
            F.col("occurrence_limit").alias("coverage_limit"),                 # Max payout per incident
            F.col("deductible_amount"),
            F.lit("COV-GL-PREM").alias("coverage_id"),
        )
        frames.append(gl)
    except Exception:
        pass

    # ---- Umbrella Premiums ----
    # Umbrella uses umbrella_limit (excess coverage above underlying policies)
    # retention_amount = the amount the insured retains before umbrella kicks in
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.col("policy_id"),
            F.lit("UMB").alias("lob_code"),
            F.col("effective_date"),
            F.col("premium_amount").alias("written_premium"),
            F.col("umbrella_limit").alias("coverage_limit"),                   # Total excess limit
            F.col("retention_amount").alias("deductible_amount"),              # Self-insured retention
            F.lit("COV-UMB-EXCESS").alias("coverage_id"),
        )
        frames.append(umb)
    except Exception:
        pass

    if not frames:
        print("    WARN: No Silver data found. Skipping.")
        return None

    # Union all LOB frames into a single DataFrame
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)

    # ---- Derive premium components ----
    # TRAINEE NOTE — Industry standard calculations (simplified for this platform):
    #   earned_premium  = written_premium (simplified: assume fully earned at booking)
    #   ceded_premium   = 15% of written (typical quota share reinsurance rate)
    #   net_premium     = written - ceded (what the insurer retains after reinsurance)
    #   commission      = 12% of written (typical agent commission rate)
    #   tax             = 3% of written (state premium tax rate, varies by state)
    #   date_key        = YYYYMMDD integer FK for dim_date join
    df = (
        df.withColumn("earned_premium", F.col("written_premium"))
        .withColumn("ceded_premium", F.round(F.col("written_premium") * 0.15, 2))   # 15% to reinsurer
        .withColumn("net_premium", F.round(F.col("written_premium") - F.col("ceded_premium"), 2))
        .withColumn("commission_amount", F.round(F.col("written_premium") * 0.12, 2))  # 12% agent commission
        .withColumn("tax_amount", F.round(F.col("written_premium") * 0.03, 2))         # 3% premium tax
        .withColumn(
            "date_key",
            # Convert effective_date → integer YYYYMMDD → FK for dim_date
            F.when(F.col("effective_date").isNotNull(), F.date_format(F.col("effective_date"), "yyyyMMdd").cast("int")),
        )
    )

    # ---- Join policy dimension for policy_key and lob_key FKs ----
    # We alias policy_df["policy_id"] to "_pid" to avoid column ambiguity
    # (the fact DataFrame also has a "policy_id" column).
    if policy_df is not None:
        df = df.join(
            policy_df.select(
                F.col("policy_key"), F.col("policy_id").alias("_pid"), F.col("lob_key"), F.col("location_key")
            ),
            df["policy_id"] == F.col("_pid"),
            "left",
        ).drop("_pid")  # Remove alias after join — no longer needed
    else:
        # dim_policy not yet built — fill FK columns with null
        for c in ["policy_key", "lob_key", "location_key"]:
            df = df.withColumn(c, F.lit(None).cast("string"))

    # ---- Join coverage dimension for coverage_key FK ----
    if coverage_df is not None:
        df = df.join(
            coverage_df.select(F.col("coverage_key"), F.col("coverage_id").alias("_cov_id")),
            df["coverage_id"] == F.col("_cov_id"),
            "left",
        ).drop("_cov_id")
    else:
        df = df.withColumn("coverage_key", F.lit(None).cast("string"))

    # ---- Generate surrogate key for this fact table ----
    # TRAINEE NOTE: The fact surrogate key uniquely identifies one premium record.
    # We hash (policy_id, coverage_id, date_key) — this combination represents
    # "what coverage, for which policy, starting on which date".
    # If the same combination appears twice, it means a duplicate load — the
    # key will be the same (deterministic), so re-running is safe.
    df = generate_surrogate_key(df, "policy_id", "coverage_id", "date_key", key_name="premium_key")

    # ---- Final column selection — clean output schema ----
    df = df.select(
        "premium_key",       # Surrogate PK for this fact row
        "policy_key",        # FK → dim_policy
        "coverage_key",      # FK → dim_coverage
        "date_key",          # FK → dim_date (effective date)
        "lob_key",           # FK → dim_line_of_business
        "location_key",      # FK → dim_location
        "written_premium",   # Full annual premium billed
        "earned_premium",    # Premium earned to date (simplified = written)
        "ceded_premium",     # Passed to reinsurer (15%)
        "net_premium",       # Retained after reinsurance
        "deductible_amount", # Insured's out-of-pocket before coverage kicks in
        "coverage_limit",    # Maximum payout under this coverage
        "commission_amount", # Agent's fee (12%)
        "tax_amount",        # State premium tax (3%)
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df


def _safe_table(spark, table_name, cols=None):
    """Load a table (optionally selecting specific columns), or return None.

    TRAINEE NOTE:
    The cols parameter allows us to load only the columns we need from a
    dimension table, rather than pulling all columns across the network.
    This is a performance optimisation — especially important when dimensions
    are wide (many columns) and we only need 2-3 for the FK join.
    """
    try:
        df = spark.table(table_name)
        return df.select(cols) if cols else df
    except Exception:
        return None
