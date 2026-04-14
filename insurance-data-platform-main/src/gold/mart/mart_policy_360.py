"""
Gold Layer — mart_policy_360 Data Mart.
Denormalized wide table providing a complete 360-degree view per policy_id.
Joins all dimensions and aggregates facts into a single queryable structure.

TRAINEE GUIDE — What is a Data Mart and why is it "360-degree"?
A Data Mart is a subject-area-specific, denormalized table optimised for
business users and BI tools (Power BI, Tableau, Looker, Excel pivot tables).

"360-degree" means: ONE ROW contains EVERYTHING about a policy:
  - Who the insured is (from dim_insured)
  - Which agent sold it (from dim_agent)
  - Where the risk is located (from dim_location)
  - Which line of business (from dim_line_of_business)
  - How much premium was collected (aggregated from fact_premium)
  - How many claims were filed (aggregated from fact_claim_transaction)
  - What the loss ratio is (computed from premiums / claims)
  - LOB-specific details (construction type, vehicle year, payroll, etc.)

Star Schema (Gold dims + facts) vs. Data Mart:
  Star Schema: Normalised — dimensions are separate tables linked by keys.
               Good for flexible ad-hoc queries, storage efficiency.
               Requires joins in every query.

  Data Mart  : Denormalised — all info pre-joined into one wide table.
               Good for dashboards and reports — no joins needed at query time.
               Faster for BI tools; easier for non-SQL users to query.
               Pre-computed aggregates (total_premium, loss_ratio) save time.

The trade-off: The mart is larger in size but much faster to query.
In practice, marts are rebuilt nightly from the Gold star schema.

Grain: one row per policy_id
  This means if a policy_id appears in multiple LOBs (unlikely here since IDs
  include LOB prefix), the last join would overwrite. dim_policy ensures
  uniqueness at build time via deduplication.

Usage:
    from src.gold.mart.mart_policy_360 import build_mart_policy_360
    build_mart_policy_360(spark)

    # Query the mart
    spark.sql("SELECT * FROM insurance_catalog.gold.mart_policy_360 WHERE policy_id = 'AUTO-0000000123'")
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import write_delta_table

CATALOG = "insurance_catalog"
GOLD = "gold"
SILVER = "silver"
TABLE = "mart_policy_360"


def build_mart_policy_360(spark: SparkSession):
    """
    Build the Policy 360 data mart by joining all Gold dimensions, facts,
    and Silver LOB-specific attributes into a single denormalized table.

    TRAINEE NOTE — How to read this function:
    The function is structured in 12 clearly numbered sections. Read them in
    order to understand the progressive enrichment of the `mart` DataFrame:
      1. Load dimension tables
      2. Start with dim_policy as the base
      3-6. Join each dimension (adds WHO/WHAT/WHERE columns)
      7. Aggregate premium facts (adds HOW MUCH premium)
      8. Aggregate claim facts (adds HOW MANY / HOW MUCH claims)
      9. Compute loss ratio (a key insurance KPI)
      10. Add LOB-specific Silver attributes (LOB detail columns)
      11. Add derived fields (days until expiry, active flag)
      12. Final column selection and write

    Performance note:
    This function performs many joins and aggregations. On large datasets
    (100-200GB), Databricks will distribute this across many worker nodes.
    The overwrite mode means it is safe to re-run if it fails partway through.
    """
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")
    print("  Joining dimensions and aggregating facts...")

    # =========================================================================
    # 1. Load Gold dimensions
    # TRAINEE NOTE: We load all dimensions upfront using _safe_table().
    # If a dimension doesn't exist (e.g. during a partial pipeline run),
    # _safe_table returns None and each join section skips it gracefully.
    # =========================================================================
    dim_policy = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_policy")
    dim_insured = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_insured")
    dim_lob = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_line_of_business")
    dim_agent = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_agent")
    dim_location = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_location")

    if dim_policy is None:
        print("    ERROR: dim_policy not found. Cannot build mart.")
        return None

    # =========================================================================
    # 2. Start with dim_policy as the base
    # TRAINEE NOTE: dim_policy is the anchor of the mart — every row in the
    # mart corresponds to one row in dim_policy. All subsequent joins ADD
    # columns to this base; they do not filter or remove rows (LEFT JOINs).
    # =========================================================================
    mart = dim_policy.alias("p")

    # =========================================================================
    # 3. Join dim_insured
    # TRAINEE NOTE — Join alias technique:
    # We rename insured_key to "_ik" in the insured_cols select to avoid
    # column ambiguity during the join (mart also has "insured_key").
    # After the join, we drop "_ik" since it is now redundant.
    # All joins here are LEFT joins — a missing match results in null columns,
    # not a dropped row. This ensures every policy appears in the mart.
    # =========================================================================
    if dim_insured is not None:
        insured_cols = dim_insured.select(
            F.col("insured_key").alias("_ik"),
            F.col("insured_name"),
            F.col("insured_type"),                                  # Individual or Business
            F.col("date_of_birth").alias("insured_dob"),
            F.col("gender").alias("insured_gender"),
            F.col("ein").alias("insured_ein"),                      # Employer Identification Number (businesses)
            F.col("naics_code").alias("insured_naics"),             # Industry code (businesses)
            F.col("industry").alias("insured_industry"),
            F.col("employee_count").alias("insured_employee_count"),
            F.col("annual_revenue").alias("insured_annual_revenue"),
        )
        mart = mart.join(insured_cols, mart["insured_key"] == insured_cols["_ik"], "left").drop("_ik")

    # =========================================================================
    # 4. Join dim_line_of_business
    # Adds the LOB name, category, and NAIC code to each policy row.
    # NAIC = National Association of Insurance Commissioners (US regulator)
    # NAIC codes are used in regulatory filings.
    # =========================================================================
    if dim_lob is not None:
        lob_cols = dim_lob.select(
            F.col("lob_key").alias("_lk"),
            F.col("lob_code"),          # e.g. "PROP", "AUTO"
            F.col("lob_name"),          # e.g. "Property", "Auto"
            F.col("lob_category"),      # e.g. "Property", "Casualty"
            F.col("naic_code"),         # e.g. "04.0" for flood insurance
        )
        mart = mart.join(lob_cols, mart["lob_key"] == lob_cols["_lk"], "left").drop("_lk")

    # =========================================================================
    # 5. Join dim_agent
    # Adds the selling agent's name, agency, and commission tier.
    # Useful for agent performance analysis and commission reporting.
    # =========================================================================
    if dim_agent is not None:
        agent_cols = dim_agent.select(
            F.col("agent_key").alias("_ak"),
            F.col("agent_name"),
            F.col("agency_name"),
            F.col("agency_code"),
            F.col("commission_tier"),   # e.g. "Gold", "Silver", "Standard"
        )
        mart = mart.join(agent_cols, mart["agent_key"] == agent_cols["_ak"], "left").drop("_ak")

    # =========================================================================
    # 6. Join dim_location
    # Adds geographic information about the insured risk location.
    # Columns are prefixed with "policy_" to clarify they refer to the
    # insured location (not the insured person's home address).
    # =========================================================================
    if dim_location is not None:
        loc_cols = dim_location.select(
            F.col("location_key").alias("_lok"),
            F.col("address_line1").alias("policy_address"),
            F.col("city").alias("policy_city"),
            F.col("state_code").alias("policy_state"),
            F.col("state_name").alias("policy_state_name"),
            F.col("zip_code").alias("policy_zip"),
            F.col("latitude").alias("policy_latitude"),
            F.col("longitude").alias("policy_longitude"),
            F.col("flood_zone").alias("policy_flood_zone"),  # FEMA flood zone (A/AE/X/etc.)
        )
        mart = mart.join(loc_cols, mart["location_key"] == loc_cols["_lok"], "left").drop("_lok")

    # =========================================================================
    # 7. Aggregate premium facts
    # TRAINEE NOTE — Pre-aggregating facts at the mart grain (policy level):
    # fact_premium has one row per policy-coverage-date. The mart has one row
    # per policy. We GROUP BY policy_key and SUM/COUNT the fact columns.
    # This pre-computation means mart users don't need to write aggregation
    # queries themselves — the mart already provides ready-to-use totals.
    #
    # The try/except: if fact_premium doesn't exist yet, we fill the columns
    # with null/zero to keep the mart schema consistent.
    # =========================================================================
    try:
        premium_agg = (
            spark.table(f"{CATALOG}.{GOLD}.fact_premium")
            .groupBy("policy_key")
            .agg(
                F.sum("written_premium").alias("total_written_premium"),
                F.sum("earned_premium").alias("total_earned_premium"),
                F.sum("net_premium").alias("total_net_premium"),
                F.sum("coverage_limit").alias("total_coverage_limit"),
                F.sum("deductible_amount").alias("total_deductible"),
                F.sum("commission_amount").alias("total_commission"),
                F.count("*").alias("premium_line_count"),  # Number of coverage lines on the policy
            )
        )
        mart = mart.join(premium_agg, "policy_key", "left")
    except Exception:
        # Fact table missing — add null/zero placeholder columns
        for c in [
            "total_written_premium",
            "total_earned_premium",
            "total_net_premium",
            "total_coverage_limit",
            "total_deductible",
            "total_commission",
            "premium_line_count",
        ]:
            mart = mart.withColumn(c, F.lit(None).cast(DecimalType(18, 2)) if "count" not in c else F.lit(0))

    # =========================================================================
    # 8. Aggregate claim facts
    # TRAINEE NOTE — Two separate aggregations are needed:
    #   a) claim_agg: Financial amounts from fact_claim_transaction
    #      (paid amounts, reserves, incurred losses)
    #   b) claim_status: Open vs. closed counts from dim_claim
    #      (dim_claim stores the claim entity; fact stores the transactions)
    #
    # Insurance terminology:
    #   paid_amount     : Cash actually paid out to the insured
    #   reserve_amount  : Money set aside (reserved) for future claim payments
    #   total_incurred  : paid + reserve = best estimate of total claim cost
    #   open claim      : Still being processed / payment not final
    #   closed claim    : Fully resolved (paid or denied)
    # =========================================================================
    try:
        claim_agg = (
            spark.table(f"{CATALOG}.{GOLD}.fact_claim_transaction")
            .groupBy("policy_key")
            .agg(
                F.countDistinct("claim_key").alias("total_claims_count"),      # Unique claims (not transactions)
                F.sum("paid_amount").alias("total_paid_amount"),
                F.sum("reserve_amount").alias("total_reserved_amount"),
                F.sum("total_incurred").alias("total_incurred"),               # paid + reserve
                F.max("date_key").alias("last_claim_date_key"),                # Date of most recent claim
                F.max("paid_amount").alias("largest_claim_amount"),            # Largest single claim payment
            )
        )

        # Open vs. closed claim counts from the claim dimension
        claim_status = (
            spark.table(f"{CATALOG}.{GOLD}.dim_claim")
            .groupBy("policy_key")
            .agg(
                # F.when inside F.sum creates a conditional count:
                #   count 1 for each "Open" claim, 0 for everything else
                F.sum(F.when(F.col("claim_status") == "Open", 1).otherwise(0)).alias("open_claims_count"),
                F.sum(F.when(F.col("claim_status") == "Closed", 1).otherwise(0)).alias("closed_claims_count"),
            )
        )

        mart = mart.join(claim_agg, "policy_key", "left")
        mart = mart.join(claim_status, "policy_key", "left")
    except Exception:
        # Claim facts/dims missing — fill with zeros
        for c in [
            "total_claims_count",
            "total_paid_amount",
            "total_reserved_amount",
            "total_incurred",
            "last_claim_date_key",
            "largest_claim_amount",
            "open_claims_count",
            "closed_claims_count",
        ]:
            mart = mart.withColumn(c, F.lit(0))

    # =========================================================================
    # 9. Calculate loss ratio
    # TRAINEE NOTE — Loss Ratio is the most important KPI in insurance:
    #   Loss Ratio = Total Incurred Losses / Written Premium
    #   e.g. 0.65 means for every $1 of premium collected, $0.65 was paid in claims
    #
    # Interpretation:
    #   < 0.60  : Very profitable (but may indicate underinsurance)
    #   0.60-0.70: Target range for most P&C insurers
    #   > 1.00  : Unprofitable (claims exceed premiums — underwriting loss)
    #
    # We guard against division by zero: only compute when premium > 0.
    # We also check for null (policy has no premium records in fact table).
    # F.round(..., 4) keeps 4 decimal places (e.g. 0.6543 = 65.43% loss ratio).
    # =========================================================================
    mart = mart.withColumn(
        "loss_ratio",
        F.when(
            (F.col("total_written_premium").isNotNull()) & (F.col("total_written_premium") > 0),
            F.round(F.col("total_incurred") / F.col("total_written_premium"), 4),
        ).otherwise(F.lit(None)),  # null when no premium exists (avoid misleading 0.0)
    )

    # =========================================================================
    # 10. LOB-specific attributes from Silver
    # TRAINEE NOTE — Why go back to Silver for LOB attributes?
    # The Gold dimension tables normalise policies to a common schema, so
    # LOB-specific columns (flood_zone, vehicle_year, payroll_amount) are NOT
    # stored in dim_policy. Instead, we join back to the Silver LOB tables to
    # bring in these LOB-specific details.
    #
    # Each LOB join is wrapped in try/except. If a LOB's Silver table is
    # missing, the columns are filled with null. This ensures the mart is
    # buildable even when only some LOBs have been processed.
    #
    # Prefix convention: property_ / auto_ / wc_ / gl_ / umb_ prefixes on
    # LOB-specific columns make it clear which LOB the column belongs to
    # when looking at a row in the wide mart table.
    # =========================================================================

    # Property-specific details (construction, occupancy, floor count, coverage values)
    try:
        prop_attrs = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.col("policy_id").alias("_prop_pid"),
            F.col("construction_type").alias("property_construction_type"),    # e.g. "Frame", "Masonry"
            F.col("occupancy_type").alias("property_occupancy_type"),          # e.g. "Residential", "Commercial"
            F.col("number_of_floors").alias("property_floors"),
            F.col("total_building_coverage").alias("property_building_value"),
            F.col("total_contents_coverage").alias("property_contents_value"),
        )
        mart = mart.join(prop_attrs, mart["policy_id"] == prop_attrs["_prop_pid"], "left").drop("_prop_pid")
    except Exception:
        for c in [
            "property_construction_type",
            "property_occupancy_type",
            "property_floors",
            "property_building_value",
            "property_contents_value",
        ]:
            mart = mart.withColumn(c, F.lit(None))

    # Auto-specific details (vehicle info)
    try:
        auto_attrs = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("policy_id").alias("_auto_pid"),
            F.col("vehicle_year").alias("auto_vehicle_year"),
            F.col("vehicle_make").alias("auto_vehicle_make"),   # e.g. "Toyota"
            F.col("vehicle_model").alias("auto_vehicle_model"), # e.g. "Camry"
            F.col("vin").alias("auto_vin"),                     # Vehicle Identification Number (17 chars)
            F.col("garage_state").alias("auto_garage_state"),   # State where vehicle is garaged (affects rates)
        )
        mart = mart.join(auto_attrs, mart["policy_id"] == auto_attrs["_auto_pid"], "left").drop("_auto_pid")
    except Exception:
        for c in ["auto_vehicle_year", "auto_vehicle_make", "auto_vehicle_model", "auto_vin", "auto_garage_state"]:
            mart = mart.withColumn(c, F.lit(None))

    # Workers' Comp-specific details (employer/payroll info + injury count)
    try:
        wc_attrs = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_policies").select(
            F.col("policy_id").alias("_wc_pid"),
            F.col("class_code").alias("wc_class_code"),             # NCCI class code (occupation type)
            F.col("payroll_amount").alias("wc_payroll_amount"),     # Employer's payroll (WC premium base)
            F.col("experience_mod_factor").alias("wc_experience_mod"),  # Experience modification (<1 = good, >1 = bad history)
        )
        # Count total injuries from the claims table (separate query, grouped to policy level)
        wc_injury_count = (
            spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_claims")
            .groupBy("policy_id")
            .agg(F.count("*").alias("wc_injury_count"))
            .withColumnRenamed("policy_id", "_wc_inj_pid")
        )
        mart = mart.join(wc_attrs, mart["policy_id"] == wc_attrs["_wc_pid"], "left").drop("_wc_pid")
        mart = mart.join(wc_injury_count, mart["policy_id"] == wc_injury_count["_wc_inj_pid"], "left").drop(
            "_wc_inj_pid"
        )
    except Exception:
        for c in ["wc_class_code", "wc_payroll_amount", "wc_experience_mod", "wc_injury_count"]:
            mart = mart.withColumn(c, F.lit(None))

    # General Liability-specific details (business info + limits)
    try:
        gl_attrs = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("policy_id").alias("_gl_pid"),
            F.col("business_type").alias("gl_business_type"),      # e.g. "Retail", "Restaurant"
            F.col("revenue").alias("gl_revenue"),                   # Annual revenue (GL premium base)
            F.col("occurrence_limit").alias("gl_occurrence_limit"), # Max payout per single event
            F.col("aggregate_limit").alias("gl_aggregate_limit"),   # Max total payout per policy year
        )
        mart = mart.join(gl_attrs, mart["policy_id"] == gl_attrs["_gl_pid"], "left").drop("_gl_pid")
    except Exception:
        for c in ["gl_business_type", "gl_revenue", "gl_occurrence_limit", "gl_aggregate_limit"]:
            mart = mart.withColumn(c, F.lit(None))

    # Umbrella-specific details (excess limits + underlying policies)
    try:
        umb_attrs = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.col("policy_id").alias("_umb_pid"),
            F.col("umbrella_limit").alias("umb_umbrella_limit"),            # Total excess coverage amount
            F.col("retention_amount").alias("umb_retention_amount"),        # Self-insured retention
            F.col("underlying_auto_policy_id").alias("umb_underlying_auto"),  # Auto policy this umbrella covers
            F.col("underlying_gl_policy_id").alias("umb_underlying_gl"),    # GL policy this umbrella covers
            F.col("underlying_wc_policy_id").alias("umb_underlying_wc"),    # WC policy this umbrella covers
        )
        mart = mart.join(umb_attrs, mart["policy_id"] == umb_attrs["_umb_pid"], "left").drop("_umb_pid")
    except Exception:
        for c in [
            "umb_umbrella_limit",
            "umb_retention_amount",
            "umb_underlying_auto",
            "umb_underlying_gl",
            "umb_underlying_wc",
        ]:
            mart = mart.withColumn(c, F.lit(None))

    # =========================================================================
    # 11. Derived fields
    # TRAINEE NOTE — Computed columns that are useful for dashboards:
    #   policy_tenure_months : How long the policy has been in force.
    #                          Here we reuse policy_term_months (the contracted
    #                          duration). A more precise version would use
    #                          CURRENT_DATE - effective_date.
    #
    #   days_until_expiry    : How many days until the policy expires.
    #     F.to_date(expiration_date_key.cast("string"), "yyyyMMdd") converts
    #     the integer 20250115 back to a date. Then F.datediff gives the gap
    #     from today. Negative values mean the policy has already expired.
    #
    #   is_active            : True if the policy hasn't expired yet.
    #     Simple boolean derived from days_until_expiry > 0. BI tools can
    #     filter to is_active = True for current in-force portfolio reports.
    # =========================================================================
    mart = (
        mart.withColumn("policy_tenure_months", F.col("policy_term_months"))
        .withColumn(
            "days_until_expiry",
            F.datediff(F.to_date(F.col("expiration_date_key").cast("string"), "yyyyMMdd"), F.current_date()),
        )
        .withColumn("is_active", F.col("days_until_expiry") > 0)
    )

    # =========================================================================
    # 12. Final column selection and write
    # TRAINEE NOTE: We explicitly select and ORDER all output columns.
    # This serves two purposes:
    #   1. Drops any intermediate helper columns that leaked through.
    #   2. Enforces a predictable, documented schema for downstream consumers.
    # Columns are grouped by category (policy core, LOB, insured, agent, etc.)
    # to make it easy to find what you need in a 60+ column wide table.
    # =========================================================================
    mart = mart.select(
        # --- Policy core identity ---
        "policy_key",
        "policy_id",
        "policy_number",
        "effective_date_key",
        "expiration_date_key",
        "policy_status",
        "policy_term_months",
        "risk_tier",
        "is_renewal",
        "underwriter",
        # --- Line of Business ---
        "lob_key",
        "lob_code",
        "lob_name",
        "lob_category",
        "naic_code",
        # --- Insured (who is covered) ---
        "insured_key",
        "insured_name",
        "insured_type",
        "insured_dob",
        "insured_gender",
        "insured_ein",
        "insured_naics",
        "insured_industry",
        "insured_employee_count",
        "insured_annual_revenue",
        # --- Agent (who sold it) ---
        "agent_key",
        "agent_name",
        "agency_name",
        "agency_code",
        "commission_tier",
        # --- Location (where is the risk) ---
        "location_key",
        "policy_address",
        "policy_city",
        "policy_state",
        "policy_state_name",
        "policy_zip",
        "policy_latitude",
        "policy_longitude",
        "policy_flood_zone",
        # --- Premium summary (aggregated from fact_premium) ---
        "total_written_premium",
        "total_earned_premium",
        "total_net_premium",
        "total_coverage_limit",
        "total_deductible",
        "total_commission",
        "premium_line_count",
        # --- Claims summary (aggregated from fact_claim_transaction) ---
        "total_claims_count",
        "open_claims_count",
        "closed_claims_count",
        "total_paid_amount",
        "total_reserved_amount",
        "total_incurred",
        "loss_ratio",               # KEY KPI: total_incurred / total_written_premium
        "last_claim_date_key",
        "largest_claim_amount",
        # --- Property-specific ---
        "property_construction_type",
        "property_occupancy_type",
        "property_floors",
        "property_building_value",
        "property_contents_value",
        # --- Auto-specific ---
        "auto_vehicle_year",
        "auto_vehicle_make",
        "auto_vehicle_model",
        "auto_vin",
        "auto_garage_state",
        # --- Workers' Comp-specific ---
        "wc_class_code",
        "wc_payroll_amount",
        "wc_experience_mod",
        "wc_injury_count",
        # --- General Liability-specific ---
        "gl_business_type",
        "gl_revenue",
        "gl_occurrence_limit",
        "gl_aggregate_limit",
        # --- Umbrella-specific ---
        "umb_umbrella_limit",
        "umb_retention_amount",
        "umb_underlying_auto",
        "umb_underlying_gl",
        "umb_underlying_wc",
        # --- Derived / computed ---
        "policy_tenure_months",
        "days_until_expiry",
        "is_active",                # True if policy has not expired today
    )

    write_delta_table(mart, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {mart.count():,}")
    print("\n    mart_policy_360 built successfully!")
    print(f"    Query: SELECT * FROM {CATALOG}.{GOLD}.{TABLE} WHERE policy_id = '<your_policy_id>'")
    return mart


def _safe_table(spark, table_name):
    """Load a table if it exists, return None if it doesn't.

    TRAINEE NOTE:
    Same helper as in other Gold modules. The mart build is tolerant of
    missing tables at every step — this is important during development when
    not all dimensions/facts may have been built yet.
    """
    try:
        return spark.table(table_name)
    except Exception:
        return None
