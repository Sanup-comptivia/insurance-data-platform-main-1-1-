"""
Silver Layer — Workers' Compensation Transformations.
Cleanses, types, deduplicates, and validates WC policy and claim data.

TRAINEE GUIDE — What is Workers' Compensation (WC) Insurance?
Workers' Compensation covers workplace injuries and illnesses. If an
employee is hurt on the job (e.g. back injury from lifting, repetitive
stress, slip-and-fall), WC pays for their medical bills and lost wages.

Key concepts:
  - WC is MANDATED by state law — almost every employer must carry it.
  - employer       : The business that employs the injured worker.
  - payroll        : Total employee payroll, which is the basis for premium
                     calculation (higher payroll = more exposure = higher premium).
  - class_code     : NCCI classification code that groups jobs by risk level
                     (e.g. office worker vs. roofer). Determines the base rate.
  - experience_mod_factor : A multiplier (typically 0.70 – 1.50) that adjusts
                     premium based on the employer's own claim history vs. peers.
                     Below 1.0 = better than average; above 1.0 = worse.

WC Claims involve:
  - injury_type    : The kind of injury (e.g. strain, fracture, burn)
  - body_part      : Which body part was injured (e.g. back, hand, knee)
  - medical_paid   : Healthcare costs for treating the workplace injury
  - indemnity_paid : Wage replacement payments while the employee cannot work

Usage:
    from src.silver.transform_workers_comp import WorkersCompTransformer
    transformer = WorkersCompTransformer(spark)
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


class WorkersCompTransformer:
    """Transform workers' comp data from Bronze to Silver."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_all(self):
        """Run all WC transformations.

        TRAINEE NOTE:
        This orchestration method calls transform_policies() and
        transform_claims() in sequence. Policies must be processed before
        claims in Gold (claims reference policies), but at the Silver level
        the order does not matter since each Silver table is independent.
        """
        print("Silver Layer — Workers' Compensation Transformations")
        print("=" * 60)
        self.transform_policies()
        self.transform_claims()
        print("\nWorkers' Comp Silver transformations complete.")

    def transform_policies(self):
        """Transform WC policies: Bronze → Silver.

        TRAINEE NOTE — The 7-step Bronze-to-Silver pipeline:
        Every Silver transformation follows this same pattern:
          Step 1: Read from Bronze (may union multiple source tables)
          Step 2: Standardize column names to snake_case
          Step 3: Cast types (strings → dates, decimals, booleans)
          Step 4: Select canonical columns (drop unwanted raw columns)
          Step 5: Deduplicate (one row per policy_id, keep newest)
          Step 6: Apply DQ checks (flag bad rows)
          Step 7: Write to Silver Delta table

        WC-specific fields explained:
          fein                : Federal Employer ID Number — like a Social
                                Security Number but for businesses (9 digits).
          naics_code          : North American Industry Classification System
                                code — identifies the employer's industry.
          class_code          : NCCI classification code that determines the
                                base premium rate per $100 of payroll. A roofer
                                (class 5551) has a much higher rate than a
                                clerical worker (class 8810).
          experience_mod_factor : Multiplier that adjusts premium based on the
                                employer's own claim history vs. industry peers.
                                DecimalType(6,3) allows values like 0.850 or 1.250.
          payroll_amount      : Total employee payroll — the exposure base for WC.
                                WC premium formula: (payroll / 100) × class_rate × experience_mod

        Note: WC policies come from a single CSV source (not CSV + JSON like
        Property), so we use _read_bronze() instead of a union pattern.
        """
        print("\n  Transforming: silver_workers_comp_policies")

        # Step 1: Read from Bronze — single CSV source for WC policies
        df = self._read_bronze("bronze_workers_comp_policies_csv")
        if df is None:
            return

        # Step 2: Standardize column names (camelCase → snake_case)
        df = standardize_column_names(df)

        # Step 3: Type casting — convert raw strings to proper types
        df = (
            df.withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("policy_number", F.trim(F.col("policy_number")))
            .withColumn("effective_date", F.to_date(F.col("effective_date")))
            .withColumn("expiration_date", F.to_date(F.col("expiration_date")))
            .withColumn("employer_name", F.trim(F.col("employer_name")))
            # coalesce: use insured_name if present, otherwise fall back to employer_name
            .withColumn("insured_name", F.trim(F.coalesce(F.col("insured_name"), F.col("employer_name"))))
            .withColumn("fein", F.trim(F.col("fein")))                                    # Federal Employer ID
            .withColumn("naics_code", F.trim(F.col("naics_code")))                          # Industry classification
            .withColumn("class_code", F.trim(F.col("class_code")))                          # NCCI risk class code
            .withColumn("state_code", F.upper(F.trim(F.col("state_code"))))                  # Normalize state code
            .withColumn("payroll_amount", F.col("payroll_amount").cast(DecimalType(18, 2)))  # Premium basis
            .withColumn("experience_mod_factor", F.col("experience_mod_factor").cast(DecimalType(6, 3)))  # Mod factor
            .withColumn("premium_amount", F.col("premium_amount").cast(DecimalType(18, 2)))
            .withColumn("agent_id", F.trim(F.col("agent_id")))
            .withColumn("agent_name", F.trim(F.col("agent_name")))
            .withColumn("underwriter", F.trim(F.col("underwriter")))
        )

        # Step 4: Select canonical (standard) columns
        cols = [
            "policy_id",
            "policy_number",
            "effective_date",
            "expiration_date",
            "employer_name",
            "insured_name",
            "fein",
            "naics_code",
            "class_code",
            "state_code",
            "payroll_amount",
            "experience_mod_factor",
            "premium_amount",
            "agent_id",
            "agent_name",
            "underwriter",
            "_ingestion_timestamp",  # Kept from Bronze for deduplication ordering
        ]
        df = self._select_available(df, cols)

        # Step 5: Deduplicate — keep the most recent record per policy_id
        df = deduplicate(df, key_columns=["policy_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — These checks are specific to Workers' Comp:
        #   policy_id must not be null (primary key)
        #   employer_name must not be null (WC is employer-based coverage)
        #   state_code must not be null (WC rules vary by state)
        #   payroll_amount must be positive (it is the premium basis)
        #   premium_amount must be positive (financial amount)
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("policy_id")
            .check_not_null("employer_name", "missing_employer")
            .check_not_null("state_code", "missing_state")
            .check_positive("payroll_amount")
            .check_positive("premium_amount")
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_workers_comp_policies", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def transform_claims(self):
        """Transform WC claims: Bronze → Silver.

        TRAINEE NOTE — WC Claims vs. Policies
        Policies define the WC insurance contract (employer, payroll, class).
        Claims are filed when a worker is injured on the job.

        WC-specific claim fields:
          injury_type         : Type of injury — follows standardized WC codes
                                (e.g. strain, fracture, contusion, burn).
          body_part           : Which body part was injured — also standardized
                                (e.g. lower back, hand, knee, shoulder).
          medical_paid        : Healthcare costs for treating the workplace injury
                                (doctor visits, surgery, physical therapy, medication).
          indemnity_paid      : Wage replacement payments while the employee
                                cannot work. Typically 2/3 of average weekly wage.
          total_incurred      : Total expected cost = medical + indemnity + expenses.
          return_to_work_date : Date the employee returned to work. NULL if the
                                employee is still on leave or permanently disabled.
          claim_status        : Must be one of ["Open", "Closed", "Reopened", "Pending"].
                                "Reopened" occurs when a closed claim's condition worsens.
        """
        print("\n  Transforming: silver_workers_comp_claims")

        # Step 1: Read from Bronze — WC claims come from JSON source
        df = self._read_bronze("bronze_workers_comp_claims_json")
        if df is None:
            return

        # Step 2: Standardize column names
        df = standardize_column_names(df)

        # Step 3: Type casting — claim-specific columns
        df = (
            df.withColumn("claim_id", F.trim(F.col("claim_id")))
            .withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("date_of_injury", F.to_date(F.col("date_of_injury")))
            .withColumn("injury_type", F.trim(F.col("injury_type")))
            .withColumn("body_part", F.trim(F.col("body_part")))
            .withColumn("nature_of_injury", F.trim(F.col("nature_of_injury")))
            .withColumn("cause_of_injury", F.trim(F.col("cause_of_injury")))
            .withColumn("employee_age", F.col("employee_age").cast(IntegerType()))
            .withColumn("employee_gender", F.upper(F.trim(F.col("employee_gender"))))
            .withColumn("medical_paid", F.col("medical_paid").cast(DecimalType(18, 2)))        # Healthcare costs
            .withColumn("indemnity_paid", F.col("indemnity_paid").cast(DecimalType(18, 2)))  # Wage replacement
            .withColumn("total_incurred", F.col("total_incurred").cast(DecimalType(18, 2)))  # medical + indemnity + expenses
            .withColumn("claim_status", F.trim(F.col("claim_status")))
            .withColumn("return_to_work_date", F.to_date(F.col("return_to_work_date")))     # null if still on leave
        )

        # Step 4: Select canonical columns
        cols = [
            "claim_id",
            "policy_id",
            "date_of_injury",
            "injury_type",
            "body_part",
            "nature_of_injury",
            "cause_of_injury",
            "employee_age",
            "employee_gender",
            "medical_paid",
            "indemnity_paid",
            "total_incurred",
            "claim_status",
            "return_to_work_date",
            "_ingestion_timestamp",
        ]
        df = self._select_available(df, cols)

        # Step 5: Deduplicate by claim_id — one record per claim
        df = deduplicate(df, key_columns=["claim_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — claim_status is validated against a fixed set:
        #   Open     = claim is being actively processed
        #   Closed   = claim is settled and finalized
        #   Reopened = previously closed claim where condition worsened
        #   Pending  = claim filed but not yet under active processing
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("claim_id")
            .check_not_null("date_of_injury", "missing_injury_date")
            .check_in_set("claim_status", ["Open", "Closed", "Reopened", "Pending"])
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_workers_comp_claims", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def _read_bronze(self, table_name):
        """Read a single Bronze table, returning None if it doesn't exist.

        TRAINEE NOTE:
        Unlike Property (which unions CSV + JSON), WC reads from a single
        Bronze source per entity type. This helper handles the case where
        the table hasn't been created yet (early development, partial runs).
        """
        try:
            return self.spark.table(f"{CATALOG}.{BRONZE}.{table_name}")
        except Exception as e:
            print(f"    WARN: {table_name} not found — {e}")
            return None

    def _select_available(self, df, columns):
        """Select only columns that exist in the DataFrame.

        TRAINEE NOTE:
        This defensive helper prevents errors when a source does not contain
        every column in our canonical list. Instead of raising a ColumnNotFound
        error, we simply skip the missing columns and select what is available.
        """
        return df.select([c for c in columns if c in df.columns])
