"""
Silver Layer — General Liability Transformations.
Cleanses, types, deduplicates, and validates GL policy and claim data.

TRAINEE GUIDE — What is General Liability (GL) Insurance?
GL covers businesses against third-party claims — injuries or damage
suffered by NON-EMPLOYEES on the business premises or from its products.
Think: a customer slips on a wet floor, or a product injures a consumer.

GL is DISTINCT from Workers' Comp:
  - WC covers EMPLOYEES injured on the job.
  - GL covers NON-EMPLOYEE third parties (customers, visitors, the public).

Key concepts:
  - business_type      : Type of business (restaurant, retail, contractor, etc.)
  - revenue            : Annual business revenue — the exposure base for premium
                         calculation (more revenue = more customer interactions = more risk).
  - occurrence_limit   : Maximum payout per SINGLE event (e.g. one slip-and-fall).
  - aggregate_limit    : Maximum total payout per POLICY YEAR across all events.
                         Typically set at 2x the occurrence limit.
                         Example: $1M occurrence / $2M aggregate means the insurer
                         will pay up to $1M for any one event but no more than $2M
                         total for all events in the year.

Common GL claim types:
  - Bodily Injury     : Physical injury to a third party (e.g. slip-and-fall)
  - Property Damage   : Damage to someone else's property
  - Personal Injury   : Non-physical harm (defamation, invasion of privacy)
  - Advertising       : Injury from advertising activities (copyright infringement)
  - Product Liability : Injury caused by the insured's product

Usage:
    from src.silver.transform_general_liability import GeneralLiabilityTransformer
    transformer = GeneralLiabilityTransformer(spark)
    transformer.transform_all()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, BooleanType
from src.common.utils import standardize_column_names, deduplicate, write_delta_table
from src.common.data_quality import DataQualityChecker

CATALOG = "insurance_catalog"
BRONZE = "bronze"
SILVER = "silver"


class GeneralLiabilityTransformer:
    """Transform general liability data from Bronze to Silver."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_all(self):
        """Run all GL transformations.

        TRAINEE NOTE:
        This orchestration method calls transform_policies() and
        transform_claims() in sequence. Policies must be processed before
        claims in Gold (claims reference policies), but at the Silver level
        the order does not matter since each Silver table is independent.
        """
        print("Silver Layer — General Liability Transformations")
        print("=" * 60)
        self.transform_policies()
        self.transform_claims()
        print("\nGeneral Liability Silver transformations complete.")

    def transform_policies(self):
        """Transform GL policies: Bronze → Silver.

        TRAINEE NOTE — The 7-step Bronze-to-Silver pipeline:
        Every Silver transformation follows this same pattern:
          Step 1: Read from Bronze (may union multiple source tables)
          Step 2: Standardize column names to snake_case
          Step 3: Cast types (strings → dates, decimals, booleans)
          Step 4: Select canonical columns (drop unwanted raw columns)
          Step 5: Deduplicate (one row per policy_id, keep newest)
          Step 6: Apply DQ checks (flag bad rows)
          Step 7: Write to Silver Delta table

        GL-specific fields explained:
          revenue             : Annual business revenue — the exposure base for GL.
                                More revenue typically means more customer interaction,
                                which means more exposure to liability claims.
          occurrence_limit    : Max payout per single event (e.g. $1,000,000).
          aggregate_limit     : Max total payout across ALL events in one policy year.
                                Typically 2x the occurrence limit ($2,000,000).
                                Once the aggregate is exhausted, the policy stops paying.
          deductible_amount   : Amount the insured pays out-of-pocket before coverage kicks in.

        Note: GL policies come from a single CSV source, so we read directly
        rather than using a union pattern.
        """
        print("\n  Transforming: silver_general_liability_policies")

        # Step 1: Read from Bronze — single CSV source for GL policies
        try:
            df = self.spark.table(f"{CATALOG}.{BRONZE}.bronze_general_liability_policies_csv")
        except Exception as e:
            print(f"    WARN: No source data — {e}")
            return

        # Step 2: Standardize column names (camelCase → snake_case)
        df = standardize_column_names(df)

        # Step 3: Type casting — convert raw strings to proper types
        df = (
            df.withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("policy_number", F.trim(F.col("policy_number")))
            .withColumn("effective_date", F.to_date(F.col("effective_date")))
            .withColumn("expiration_date", F.to_date(F.col("expiration_date")))
            .withColumn("insured_name", F.trim(F.col("insured_name")))
            .withColumn("business_type", F.trim(F.col("business_type")))
            .withColumn("naics_code", F.trim(F.col("naics_code")))
            .withColumn("class_code", F.trim(F.col("class_code")))
            .withColumn("state_code", F.upper(F.trim(F.col("state_code"))))
            .withColumn("revenue", F.col("revenue").cast(DecimalType(18, 2)))                  # Premium basis (exposure)
            .withColumn("premium_amount", F.col("premium_amount").cast(DecimalType(18, 2)))
            .withColumn("occurrence_limit", F.col("occurrence_limit").cast(DecimalType(18, 2)))  # Max per event
            .withColumn("aggregate_limit", F.col("aggregate_limit").cast(DecimalType(18, 2)))    # Max per year (typically 2x occurrence)
            .withColumn("deductible_amount", F.col("deductible_amount").cast(DecimalType(18, 2)))
            .withColumn("agent_id", F.trim(F.col("agent_id")))
            .withColumn("agent_name", F.trim(F.col("agent_name")))
            .withColumn("underwriter", F.trim(F.col("underwriter")))
            .withColumn("address", F.trim(F.col("address")))
            .withColumn("city", F.trim(F.col("city")))
            .withColumn("state", F.upper(F.trim(F.col("state"))))
            .withColumn("zip_code", F.trim(F.col("zip_code")))
        )

        # Step 4: Select canonical (standard) columns
        cols = [
            "policy_id",
            "policy_number",
            "effective_date",
            "expiration_date",
            "insured_name",
            "business_type",
            "naics_code",
            "class_code",
            "state_code",
            "revenue",
            "premium_amount",
            "occurrence_limit",
            "aggregate_limit",
            "deductible_amount",
            "agent_id",
            "agent_name",
            "underwriter",
            "address",
            "city",
            "state",
            "zip_code",
            "_ingestion_timestamp",  # Kept from Bronze for deduplication ordering
        ]
        df = df.select([c for c in cols if c in df.columns])

        # Step 5: Deduplicate — keep the most recent record per policy_id
        df = deduplicate(df, key_columns=["policy_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — GL-specific DQ rules:
        #   policy_id must not be null (primary key)
        #   insured_name must not be null (identifies the business)
        #   occurrence_limit must be positive (coverage amount per event)
        #   premium_amount must be positive (financial amount)
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("policy_id")
            .check_not_null("insured_name", "missing_insured")
            .check_positive("occurrence_limit")
            .check_positive("premium_amount")
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_general_liability_policies", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def transform_claims(self):
        """Transform GL claims: Bronze → Silver.

        TRAINEE NOTE — GL Claims
        GL claims are filed when a third party is injured or their property
        is damaged due to the insured business's operations or products.

        GL-specific claim fields:
          incurred_amount     : Total EXPECTED cost of the claim (what we think
                                it will ultimately cost). This is an estimate that
                                gets refined over time as the claim develops.
          paid_amount         : Amount ACTUALLY paid so far. As payments are made,
                                paid_amount increases toward incurred_amount.
          reserve_amount      : Amount set aside for FUTURE payments on this claim.
                                Formula: reserve = incurred - paid.
                                Reserves are critical for financial reporting —
                                they represent the insurer's expected future obligation.
          litigation_flag     : Boolean — whether a lawsuit has been filed.
                                Litigated claims cost significantly more (legal fees,
                                higher settlements) and take longer to resolve.
          claim_type          : Category of GL claim, validated against known types:
                                BodilyInjury, PropertyDamage, PersonalInjury,
                                Advertising, ProductLiability.
          claimant_name       : The third party who filed the claim (not the insured).
        """
        print("\n  Transforming: silver_general_liability_claims")

        # Step 1: Read from Bronze — GL claims from JSON source
        try:
            df = self.spark.table(f"{CATALOG}.{BRONZE}.bronze_general_liability_claims_json")
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
            .withColumn("claim_type", F.trim(F.col("claim_type")))
            .withColumn("claim_description", F.trim(F.col("claim_description")))
            .withColumn("claimant_name", F.trim(F.col("claimant_name")))
            .withColumn("incurred_amount", F.col("incurred_amount").cast(DecimalType(18, 2)))    # Total expected cost
            .withColumn("paid_amount", F.col("paid_amount").cast(DecimalType(18, 2)))            # Actually paid so far
            .withColumn("reserve_amount", F.col("reserve_amount").cast(DecimalType(18, 2)))      # Set aside for future (incurred - paid)
            .withColumn("claim_status", F.trim(F.col("claim_status")))
            .withColumn("litigation_flag", F.col("litigation_flag").cast(BooleanType()))          # Lawsuit filed?
        )

        # Step 4: Select canonical columns
        cols = [
            "claim_id",
            "policy_id",
            "date_of_occurrence",
            "claim_type",
            "claim_description",
            "claimant_name",
            "incurred_amount",
            "paid_amount",
            "reserve_amount",
            "claim_status",
            "litigation_flag",
            "_ingestion_timestamp",
        ]
        df = df.select([c for c in cols if c in df.columns])

        # Step 5: Deduplicate by claim_id — one record per claim
        df = deduplicate(df, key_columns=["claim_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — claim_type is validated against known GL categories:
        #   BodilyInjury     = physical injury to a third party
        #   PropertyDamage   = damage to someone else's property
        #   PersonalInjury   = non-physical harm (defamation, privacy violation)
        #   Advertising      = harm from advertising (copyright infringement in ads)
        #   ProductLiability = injury caused by the insured's product
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("claim_id")
            .check_in_set(
                "claim_type", ["BodilyInjury", "PropertyDamage", "PersonalInjury", "Advertising", "ProductLiability"]
            )
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_general_liability_claims", mode="overwrite")
        print(f"    Rows: {df.count():,}")
