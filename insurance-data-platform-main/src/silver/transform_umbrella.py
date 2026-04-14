"""
Silver Layer — Umbrella Insurance Transformations.
Cleanses, types, deduplicates, and validates umbrella policy and claim data.

TRAINEE GUIDE — What is Umbrella Insurance?
Umbrella insurance is EXCESS liability coverage that sits ON TOP of
underlying policies. It pays out ONLY after the underlying policy's
limits are exhausted.

How it works (example):
  1. An insured has an Auto policy with a $250K liability limit.
  2. An accident results in a $500K judgment against the insured.
  3. The Auto policy pays $250K (its full limit).
  4. The Umbrella policy pays the remaining $250K (the "excess").

Key concepts:
  - Underlying policies  : The primary policies the umbrella sits on top of.
                           Typically: Auto, General Liability, Workers' Comp,
                           and Employers' Liability.
  - umbrella_limit       : Maximum the umbrella policy will pay in EXCESS of
                           the underlying policy limits.
  - retention_amount     : Self-Insured Retention (SIR) — the insured's
                           "deductible" for the umbrella. The insured pays this
                           amount out-of-pocket before umbrella coverage kicks in.
                           SIR typically applies when there is no underlying policy
                           for the specific type of claim.
  - insured_type         : "Individual" (personal umbrella) vs "Commercial"
                           (commercial umbrella / excess liability).

Usage:
    from src.silver.transform_umbrella import UmbrellaTransformer
    transformer = UmbrellaTransformer(spark)
    transformer.transform_all()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import standardize_column_names, deduplicate, write_delta_table
from src.common.data_quality import DataQualityChecker

CATALOG = "insurance_catalog"
BRONZE = "bronze"
SILVER = "silver"


class UmbrellaTransformer:
    """Transform umbrella insurance data from Bronze to Silver."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_all(self):
        """Run all umbrella transformations.

        TRAINEE NOTE:
        This orchestration method calls transform_policies() and
        transform_claims() in sequence. Policies must be processed before
        claims in Gold (claims reference policies), but at the Silver level
        the order does not matter since each Silver table is independent.
        """
        print("Silver Layer — Umbrella Insurance Transformations")
        print("=" * 60)
        self.transform_policies()
        self.transform_claims()
        print("\nUmbrella Silver transformations complete.")

    def transform_policies(self):
        """Transform umbrella policies: Bronze → Silver.

        TRAINEE NOTE — The 7-step Bronze-to-Silver pipeline:
        Every Silver transformation follows this same pattern:
          Step 1: Read from Bronze (may union multiple source tables)
          Step 2: Standardize column names to snake_case
          Step 3: Cast types (strings → dates, decimals, booleans)
          Step 4: Select canonical columns (drop unwanted raw columns)
          Step 5: Deduplicate (one row per policy_id, keep newest)
          Step 6: Apply DQ checks (flag bad rows)
          Step 7: Write to Silver Delta table

        Umbrella-specific fields explained:
          underlying_auto_policy_id  : FK (foreign key) to the Auto policy this
                                       umbrella sits on top of.
          underlying_gl_policy_id    : FK to the General Liability policy underneath.
          underlying_wc_policy_id    : FK to the Workers' Comp policy underneath.
                                       These FKs link the umbrella to the primary
                                       policies it extends. An umbrella may cover
                                       one, two, or all three underlying lines.
          retention_amount           : Self-Insured Retention (SIR). Like a
                                       deductible — the insured pays this amount
                                       out-of-pocket before umbrella coverage kicks in.
          umbrella_limit             : Maximum the umbrella will pay in excess of
                                       the underlying policy limits.
          insured_type               : "Individual" for personal umbrella (covers a
                                       person/family) vs "Commercial" for business
                                       umbrella/excess liability.

        Why union CSV and JSON?
          Umbrella policies arrive in two formats (same as Property and Auto):
            - CSV: Bulk data from legacy systems
            - JSON: API responses and synthetic data
          Silver unifies them into a single table with a consistent schema.
        """
        print("\n  Transforming: silver_umbrella_policies")

        # Step 1: Read from Bronze — union CSV and JSON sources
        # Both tables have the same logical data but different formats/sources
        dfs = []
        for t in ["bronze_umbrella_policies_csv", "bronze_umbrella_policies_json"]:
            try:
                dfs.append(self.spark.table(f"{CATALOG}.{BRONZE}.{t}"))
            except Exception:
                pass  # Table doesn't exist — skip it silently
        if not dfs:
            print("    WARN: No source data. Skipping.")
            return

        # Union by name to handle different column sets between CSV and JSON
        df = dfs[0]
        for extra in dfs[1:]:
            common = list(set(df.columns) & set(extra.columns))
            if common:
                df = df.select(common).unionByName(extra.select(common))

        # Step 2: Standardize column names (camelCase → snake_case)
        df = standardize_column_names(df)

        # Step 3: Type casting — convert raw strings to proper types
        df = (
            df.withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("policy_number", F.trim(F.col("policy_number")))
            .withColumn("effective_date", F.to_date(F.col("effective_date")))
            .withColumn("expiration_date", F.to_date(F.col("expiration_date")))
            .withColumn("insured_name", F.trim(F.col("insured_name")))
            .withColumn("insured_type", F.trim(F.col("insured_type")))                          # Individual or Commercial
            .withColumn("umbrella_limit", F.col("umbrella_limit").cast(DecimalType(18, 2)))  # Max excess payout
            .withColumn("retention_amount", F.col("retention_amount").cast(DecimalType(18, 2)))  # SIR (self-insured retention)
            .withColumn("premium_amount", F.col("premium_amount").cast(DecimalType(18, 2)))
            .withColumn("underlying_auto_policy_id", F.trim(F.col("underlying_auto_policy_id")))  # FK to Auto policy
            .withColumn("underlying_gl_policy_id", F.trim(F.col("underlying_gl_policy_id")))      # FK to GL policy
            .withColumn("underlying_wc_policy_id", F.trim(F.col("underlying_wc_policy_id")))      # FK to WC policy
            .withColumn("agent_id", F.trim(F.col("agent_id")))
            .withColumn("agent_name", F.trim(F.col("agent_name")))
            .withColumn("underwriter", F.trim(F.col("underwriter")))
            .withColumn("state_code", F.upper(F.trim(F.col("state_code"))))
            .withColumn("city", F.trim(F.col("city")))
            .withColumn("zip_code", F.trim(F.col("zip_code")))
        )

        # Step 4: Select canonical (standard) columns
        cols = [
            "policy_id",
            "policy_number",
            "effective_date",
            "expiration_date",
            "insured_name",
            "insured_type",
            "umbrella_limit",
            "retention_amount",
            "premium_amount",
            "underlying_auto_policy_id",
            "underlying_gl_policy_id",
            "underlying_wc_policy_id",
            "agent_id",
            "agent_name",
            "underwriter",
            "state_code",
            "city",
            "zip_code",
            "_ingestion_timestamp",  # Kept from Bronze for deduplication ordering
        ]
        df = df.select([c for c in cols if c in df.columns])

        # Step 5: Deduplicate — keep the most recent record per policy_id
        df = deduplicate(df, key_columns=["policy_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — Umbrella-specific DQ rules:
        #   policy_id must not be null (primary key)
        #   insured_name must not be null (identifies the policyholder)
        #   umbrella_limit must be positive (the excess coverage amount)
        #   retention_amount must be positive (the SIR / deductible)
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("policy_id")
            .check_not_null("insured_name", "missing_insured")
            .check_positive("umbrella_limit")
            .check_positive("retention_amount")
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_umbrella_policies", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def transform_claims(self):
        """Transform umbrella claims: Bronze → Silver.

        TRAINEE NOTE — Umbrella Claims
        Umbrella claims are triggered when an underlying policy's limits
        are exhausted. The umbrella then pays the EXCESS amount.

        Umbrella-specific claim fields:
          underlying_line     : Which underlying line of business was exhausted
                                first, triggering the umbrella. Must be one of:
                                Auto, GeneralLiability, WorkersComp, EmployersLiability.
          underlying_claim_id : The claim_id on the underlying policy that triggered
                                the umbrella payout. This creates a link back to the
                                original claim in the underlying LOB's claims table.
          excess_amount       : The portion of the claim ABOVE the underlying policy
                                limit — this is what the umbrella actually pays.
          total_claim_amount  : The ENTIRE claim cost (underlying + excess).
                                Example: If total_claim_amount = $500K and the
                                underlying Auto policy paid $250K (its limit), then
                                excess_amount = $250K (paid by umbrella).
        """
        print("\n  Transforming: silver_umbrella_claims")

        # Step 1: Read from Bronze — umbrella claims from single CSV source
        try:
            df = self.spark.table(f"{CATALOG}.{BRONZE}.bronze_umbrella_claims_csv")
        except Exception as e:
            print(f"    WARN: No source data — {e}")
            return

        # Step 2: Standardize column names
        df = standardize_column_names(df)

        # Step 3: Type casting — claim-specific columns
        df = (
            df.withColumn("claim_id", F.trim(F.col("claim_id")))
            .withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("date_of_occurrence", F.to_date(F.col("date_of_occurrence")))
            .withColumn("underlying_line", F.trim(F.col("underlying_line")))
            .withColumn("underlying_claim_id", F.trim(F.col("underlying_claim_id")))
            .withColumn("excess_amount", F.col("excess_amount").cast(DecimalType(18, 2)))
            .withColumn("total_claim_amount", F.col("total_claim_amount").cast(DecimalType(18, 2)))
            .withColumn("claim_status", F.trim(F.col("claim_status")))
        )

        cols = [
            "claim_id",
            "policy_id",
            "date_of_occurrence",
            "underlying_line",
            "underlying_claim_id",
            "excess_amount",
            "total_claim_amount",
            "claim_status",
            "_ingestion_timestamp",
        ]
        df = df.select([c for c in cols if c in df.columns])
        df = deduplicate(df, key_columns=["claim_id"])

        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("claim_id")
            .check_in_set("underlying_line", ["Auto", "GeneralLiability", "WorkersComp", "EmployersLiability"])
            .apply()
        )

        write_delta_table(df, CATALOG, SILVER, "silver_umbrella_claims", mode="overwrite")
        print(f"    Rows: {df.count():,}")
