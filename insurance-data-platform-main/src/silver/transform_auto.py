"""
Silver Layer — Auto Insurance Transformations.
Cleanses, types, deduplicates, and validates auto policy and claim data.

TRAINEE GUIDE — What is Auto Insurance?
Auto insurance covers vehicles — both personal and commercial. When a
vehicle is involved in an accident, auto insurance pays for damages to
the vehicle, other property, and bodily injuries (depending on coverage).

Key concepts:
  - Vehicle info    : year, make, model, VIN (Vehicle Identification Number)
                      uniquely identifies each insured vehicle.
  - coverage_type   : Determines what events are covered:
                        Comprehensive — theft, weather, fire, vandalism (non-collision)
                        Collision     — damage from hitting another car/object
                        Liability     — covers damage YOU cause to others
                        Full Coverage — Comprehensive + Collision + Liability
                        Minimum       — state-required minimum liability only
  - liability_limit : Maximum the insurer will pay per occurrence.
  - Premium rating  : Depends on driver age, vehicle type, location (garage_state),
                      coverage type, and driving record.

Auto Claims involve:
  - Accidents with fault determination (At_Fault, Not_At_Fault, Shared)
  - Property damage and bodily injury costs
  - Subrogation rights (recovering costs from the at-fault party)

Usage:
    from src.silver.transform_auto import AutoTransformer
    transformer = AutoTransformer(spark)
    transformer.transform_all()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from src.common.utils import standardize_column_names, deduplicate, write_delta_table
from src.common.data_quality import DataQualityChecker

CATALOG = "insurance_catalog"
BRONZE = "bronze"
SILVER = "silver"


class AutoTransformer:
    """Transform auto insurance data from Bronze to Silver."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_all(self):
        """Run all auto transformations.

        TRAINEE NOTE:
        This orchestration method calls transform_policies() and
        transform_claims() in sequence. Policies must be processed before
        claims in Gold (claims reference policies), but at the Silver level
        the order does not matter since each Silver table is independent.
        """
        print("Silver Layer — Auto Insurance Transformations")
        print("=" * 60)
        self.transform_policies()
        self.transform_claims()
        print("\nAuto Silver transformations complete.")

    def transform_policies(self):
        """Transform auto policies: Bronze → Silver.

        TRAINEE NOTE — The 7-step Bronze-to-Silver pipeline:
        Every Silver transformation follows this same pattern:
          Step 1: Read from Bronze (may union multiple source tables)
          Step 2: Standardize column names to snake_case
          Step 3: Cast types (strings → dates, decimals, booleans)
          Step 4: Select canonical columns (drop unwanted raw columns)
          Step 5: Deduplicate (one row per policy_id, keep newest)
          Step 6: Apply DQ checks (flag bad rows)
          Step 7: Write to Silver Delta table

        Auto-specific fields explained:
          insured_dob         : Date of birth of the primary driver — used for
                                age-based rating (young drivers = higher risk).
          vin                 : Vehicle Identification Number — 17-character code
                                that uniquely identifies each vehicle worldwide.
          garage_state/zip    : Where the vehicle is primarily parked/stored.
                                Determines the "rate territory" — urban areas with
                                more traffic and theft have higher premiums.
          coverage_type       : What events are covered (Comprehensive, Collision,
                                Liability, Full Coverage, Minimum).
          liability_limit     : Maximum the insurer will pay per occurrence.
          premium_amount      : Depends on driver age, vehicle type, location,
                                coverage level, and driving record.

        Why union CSV and JSON?
          Auto policies arrive in two formats (same as Property):
            - CSV: Bulk data from legacy systems
            - JSON: API responses and synthetic data
          Silver unifies them into a single table with a consistent schema.
        """
        print("\n  Transforming: silver_auto_policies")

        # Step 1: Read from Bronze — union CSV and JSON sources
        # Both tables have the same logical data but different formats/sources
        dfs = []
        for t in ["bronze_auto_policies_csv", "bronze_auto_policies_json"]:
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
            .withColumn("insured_dob", F.to_date(F.col("insured_dob")))                      # For age-based rating
            .withColumn("insured_gender", F.upper(F.trim(F.col("insured_gender"))))
            .withColumn("vehicle_year", F.col("vehicle_year").cast(IntegerType()))            # Model year
            .withColumn("vehicle_make", F.trim(F.col("vehicle_make")))                        # e.g. Toyota, Ford
            .withColumn("vehicle_model", F.trim(F.col("vehicle_model")))                      # e.g. Camry, F-150
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))                                # 17-char vehicle ID
            .withColumn("garage_state", F.upper(F.trim(F.col("garage_state"))))              # Rate territory state
            .withColumn("garage_zip", F.trim(F.col("garage_zip")))                            # Rate territory ZIP
            .withColumn("coverage_type", F.trim(F.col("coverage_type")))                      # Comp/Collision/Liability/etc.
            .withColumn("liability_limit", F.col("liability_limit").cast(DecimalType(18, 2))) # Max payout per event
            .withColumn("deductible_amount", F.col("deductible_amount").cast(DecimalType(18, 2)))
            .withColumn("premium_amount", F.col("premium_amount").cast(DecimalType(18, 2)))
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
            "insured_dob",
            "insured_gender",
            "vehicle_year",
            "vehicle_make",
            "vehicle_model",
            "vin",
            "garage_state",
            "garage_zip",
            "coverage_type",
            "liability_limit",
            "deductible_amount",
            "premium_amount",
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
        # TRAINEE NOTE — These checks are specific to auto insurance:
        #   policy_id must not be null (primary key)
        #   insured_name must not be null (identifies the policyholder/driver)
        #   premium_amount must be positive (financial amount)
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("policy_id")
            .check_not_null("insured_name", "missing_insured")
            .check_positive("premium_amount")
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_auto_policies", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def transform_claims(self):
        """Transform auto claims: Bronze → Silver.

        TRAINEE NOTE — Auto Claims
        Auto claims are filed after vehicle accidents. Key concepts:

          fault_indicator     : Determines who was at fault for the accident:
                                  At_Fault     = insured driver caused the accident
                                  Not_At_Fault = other driver caused the accident
                                  Shared       = both parties share responsibility
                                Fault determines SUBROGATION rights — the insurer's
                                right to recover claim costs from the at-fault party.
                                If our insured is Not_At_Fault, we can subrogate
                                (seek reimbursement) from the other driver's insurer.

          number_vehicles_involved : How many vehicles were in the accident.
                                More vehicles = more complex claim.
          bodily_injury_count : Number of people injured. Affects claim severity
                                and potential litigation.
          property_damage_amount : Cost of vehicle/property damage.
          total_claim_amount  : Full cost of the claim (property damage +
                                bodily injury costs + any other expenses).
        """
        print("\n  Transforming: silver_auto_claims")

        # Step 1: Read from Bronze — auto claims from single CSV source
        try:
            df = self.spark.table(f"{CATALOG}.{BRONZE}.bronze_auto_claims_csv")
        except Exception as e:
            print(f"    WARN: No source data — {e}")
            return

        # Step 2: Standardize column names
        df = standardize_column_names(df)

        # Step 3: Type casting — claim-specific columns
        df = (
            df.withColumn("claim_id", F.trim(F.col("claim_id")))
            .withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("date_of_accident", F.to_date(F.col("date_of_accident")))
            .withColumn("accident_description", F.trim(F.col("accident_description")))
            .withColumn("fault_indicator", F.trim(F.col("fault_indicator")))                    # At_Fault/Not_At_Fault/Shared
            .withColumn("number_vehicles_involved", F.col("number_vehicles_involved").cast(IntegerType()))
            .withColumn("bodily_injury_count", F.col("bodily_injury_count").cast(IntegerType()))  # People injured
            .withColumn("property_damage_amount", F.col("property_damage_amount").cast(DecimalType(18, 2)))  # Vehicle/property damage
            .withColumn("total_claim_amount", F.col("total_claim_amount").cast(DecimalType(18, 2)))  # property + bodily + expenses
            .withColumn("claim_status", F.trim(F.col("claim_status")))
        )

        # Step 4: Select canonical columns
        cols = [
            "claim_id",
            "policy_id",
            "date_of_accident",
            "accident_description",
            "fault_indicator",
            "number_vehicles_involved",
            "bodily_injury_count",
            "property_damage_amount",
            "total_claim_amount",
            "claim_status",
            "_ingestion_timestamp",
        ]
        df = df.select([c for c in cols if c in df.columns])

        # Step 5: Deduplicate by claim_id — one record per claim
        df = deduplicate(df, key_columns=["claim_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — Auto claims require:
        #   claim_id must not be null (primary key)
        #   date_of_accident must not be null (core claim date)
        dq = DataQualityChecker(df)
        df = dq.check_not_null("claim_id").check_not_null("date_of_accident", "missing_accident_date").apply()

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_auto_claims", mode="overwrite")
        print(f"    Rows: {df.count():,}")
