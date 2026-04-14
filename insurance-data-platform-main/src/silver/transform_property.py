"""
Silver Layer — Property Insurance Transformations.
Cleanses, types, deduplicates, and validates property policy and claim data.

TRAINEE GUIDE — What is the Silver Layer?
Silver is the second layer in the Medallion Architecture:
  Bronze (raw strings) → [Silver] (typed, clean) → Gold (analytics-ready)

The Silver layer answers the question: "What does the data ACTUALLY mean?"
This involves:
  1. Type casting   : Convert string "2024-01-15" → date, "1500.00" → decimal
  2. Name cleaning  : camelCase → snake_case (standardize_column_names)
  3. Deduplication  : Keep only one record per policy_id (the newest)
  4. DQ checks      : Flag bad rows without dropping them
  5. Column pruning : Select only the canonical set of columns we care about

One class per Line of Business (LOB):
  There are 5 LOB transformers: Property, Auto, Workers' Comp, GL, Umbrella.
  Each LOB has its own unique columns (e.g. Property has flood_zone;
  Auto has vehicle_year). Keeping them separate makes the code easier to
  understand and modify independently.

This file handles PROPERTY insurance — policies covering buildings, contents,
and flood damage (sourced from FEMA NFIP data and synthetic data).

Usage:
    from src.silver.transform_property import PropertyTransformer
    transformer = PropertyTransformer(spark)
    transformer.transform_all()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DecimalType, BooleanType
from src.common.utils import standardize_column_names, deduplicate, write_delta_table
from src.common.data_quality import DataQualityChecker

CATALOG = "insurance_catalog"
BRONZE = "bronze"
SILVER = "silver"


class PropertyTransformer:
    """Transform property insurance data from Bronze to Silver."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_all(self):
        """Run all property transformations.

        TRAINEE NOTE:
        This orchestration method calls transform_policies() and
        transform_claims() in sequence. Policies must be processed before
        claims in Gold (claims reference policies), but at the Silver level
        the order does not matter since each Silver table is independent.
        """
        print("Silver Layer — Property Insurance Transformations")
        print("=" * 60)
        self.transform_policies()
        self.transform_claims()
        print("\nProperty Silver transformations complete.")

    def transform_policies(self):
        """Transform property policies: Bronze → Silver.

        TRAINEE NOTE — The 7-step Bronze-to-Silver pipeline:
        Every Silver transformation follows this same pattern:
          Step 1: Read from Bronze (may union multiple source tables)
          Step 2: Standardize column names to snake_case
          Step 3: Cast types (strings → dates, decimals, booleans)
          Step 4: Select canonical columns (drop unwanted raw columns)
          Step 5: Deduplicate (one row per policy_id, keep newest)
          Step 6: Apply DQ checks (flag bad rows)
          Step 7: Write to Silver Delta table

        Why union CSV and JSON?
          Property policies arrive in two formats:
            - CSV: FEMA bulk download (historical, millions of rows)
            - JSON: Synthetic data + API responses
          Silver unifies them into a single table with a consistent schema.
          The _safe_union() helper handles the case where one source is missing.
        """
        print("\n  Transforming: silver_property_policies")

        # Step 1: Read from Bronze — union CSV and JSON sources
        # Both tables have the same logical data but different formats/sources
        csv_table = f"{CATALOG}.{BRONZE}.bronze_property_policies_csv"
        json_table = f"{CATALOG}.{BRONZE}.bronze_property_policies_json"

        df = self._safe_union(csv_table, json_table)
        if df is None:
            print("    WARN: No source data found. Skipping.")
            return

        # Step 2: Standardize column names (camelCase → snake_case)
        # e.g. "totalBuildingCoverage" → "total_building_coverage"
        df = standardize_column_names(df)

        # Step 3: Type casting
        # TRAINEE NOTE — Why cast types in Silver and not Bronze?
        # Bronze reads everything as strings to avoid type inference errors.
        # Silver applies INTENTIONAL, validated type conversions:
        #   F.trim()     : Remove leading/trailing whitespace from strings
        #   F.to_date()  : Parse "2024-01-15" string → DateType
        #   F.upper()    : Normalize state codes to uppercase ("ca" → "CA")
        #   .cast(DecimalType(18, 2)) : Convert string "$1,500.00" → Decimal
        #                               18 = total digits, 2 = decimal places
        #                               Decimal (not Double) for financial data
        #                               to avoid floating-point rounding errors
        #   .cast(BooleanType()) : "true"/"false" → boolean True/False
        df = (
            df.withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("policy_number", F.trim(F.col("policy_number")))
            .withColumn("effective_date", F.to_date(F.col("effective_date")))      # String → Date
            .withColumn("expiration_date", F.to_date(F.col("expiration_date")))    # String → Date
            .withColumn("insured_name", F.trim(F.col("insured_name")))
            .withColumn("property_state", F.upper(F.trim(F.col("property_state"))))  # Normalize state code
            .withColumn("reported_city", F.trim(F.col("reported_city")))
            .withColumn("reported_zip_code", F.trim(F.col("reported_zip_code")))
            .withColumn("latitude", F.col("latitude").cast(DoubleType()))           # GPS coordinate
            .withColumn("longitude", F.col("longitude").cast(DoubleType()))         # GPS coordinate
            .withColumn("occupancy_type", F.trim(F.col("occupancy_type")))
            .withColumn("construction_type", F.trim(F.col("construction_type")))
            .withColumn("year_built", F.col("year_built").cast(IntegerType()))      # Year as integer
            .withColumn("number_of_floors", F.col("number_of_floors").cast(IntegerType()))
            .withColumn("flood_zone", F.trim(F.col("flood_zone")))                  # FEMA flood zone code
            .withColumn("total_building_coverage", F.col("total_building_coverage").cast(DecimalType(18, 2)))
            .withColumn("total_contents_coverage", F.col("total_contents_coverage").cast(DecimalType(18, 2)))
            .withColumn("building_value", F.col("building_value").cast(DecimalType(18, 2)))
            .withColumn("contents_value", F.col("contents_value").cast(DecimalType(18, 2)))
            .withColumn("premium_amount", F.col("premium_amount").cast(DecimalType(18, 2)))
            .withColumn("deductible_amount", F.col("deductible_amount").cast(DecimalType(18, 2)))
            .withColumn("primary_residence", F.col("primary_residence").cast(BooleanType()))
            .withColumn("agent_id", F.trim(F.col("agent_id")))
            .withColumn("agent_name", F.trim(F.col("agent_name")))
            .withColumn("underwriter", F.trim(F.col("underwriter")))
        )

        # Step 4: Select canonical (standard) columns
        # TRAINEE NOTE — Why not just keep all columns from Bronze?
        # Different Bronze sources (CSV vs JSON) may have different extra
        # columns that don't belong in our canonical Silver schema. By
        # explicitly selecting only the columns we care about, we ensure
        # Silver has a consistent schema regardless of which sources were loaded.
        # _select_available() gracefully skips columns that don't exist
        # (in case a source doesn't have every field).
        canonical_cols = [
            "policy_id",
            "policy_number",
            "effective_date",
            "expiration_date",
            "insured_name",
            "property_state",
            "reported_city",
            "reported_zip_code",
            "latitude",
            "longitude",
            "occupancy_type",
            "construction_type",
            "year_built",
            "number_of_floors",
            "flood_zone",
            "total_building_coverage",
            "total_contents_coverage",
            "building_value",
            "contents_value",
            "premium_amount",
            "deductible_amount",
            "primary_residence",
            "agent_id",
            "agent_name",
            "underwriter",
            "_ingestion_timestamp",  # Kept from Bronze for deduplication ordering
        ]
        df = self._select_available(df, canonical_cols)

        # Step 5: Deduplicate — keep the most recent record per policy_id
        # If the same policy_id appears in both CSV and JSON, we keep the
        # row with the latest _ingestion_timestamp (the most recent load).
        df = deduplicate(df, key_columns=["policy_id"])

        # Step 6: Data quality checks
        # TRAINEE NOTE — These checks are specific to property insurance:
        #   policy_id must not be null (it's the primary key)
        #   property_state must not be null (required for regulatory reporting)
        #   total_building_coverage must not be negative (financial amount)
        #   premium_amount must not be negative (financial amount)
        # Bad rows are FLAGGED (not dropped) — see DataQualityChecker docs.
        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("policy_id")
            .check_not_null("property_state", "missing_state")
            .check_positive("total_building_coverage")
            .check_positive("premium_amount")
            .apply()
        )

        # Step 7: Write to Silver
        write_delta_table(df, CATALOG, SILVER, "silver_property_policies", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def transform_claims(self):
        """Transform property claims: Bronze → Silver.

        TRAINEE NOTE — Claims vs. Policies
        Policies define the insurance contract (what is covered and for how much).
        Claims are events where the insured files for payment under their policy.

        One policy can have ZERO or MANY claims over its lifetime.
        The link between claims and policies is policy_id (foreign key).

        Property-specific claim fields:
          date_of_loss        : When the damage occurred (not when reported)
          year_of_loss        : Extracted from date_of_loss for easy aggregation
          cause_of_damage     : e.g. "flood", "wind", "fire"
          amount_paid_building: Payment for building damage
          amount_paid_contents: Payment for contents (furniture, belongings, etc.)

        Note: We use F.coalesce(year_of_loss, F.year(date_of_loss)) for
        year_of_loss. This means: use year_of_loss if it exists in the data,
        otherwise derive it from date_of_loss. F.coalesce returns the first
        non-null value from its arguments.
        """
        print("\n  Transforming: silver_property_claims")

        csv_table = f"{CATALOG}.{BRONZE}.bronze_property_claims_csv"
        json_table = f"{CATALOG}.{BRONZE}.bronze_property_claims_json"

        df = self._safe_union(csv_table, json_table)
        if df is None:
            print("    WARN: No source data found. Skipping.")
            return

        df = standardize_column_names(df)

        # Type casting — same pattern as policies, but claim-specific columns
        df = (
            df.withColumn("claim_id", F.trim(F.col("claim_id")))
            .withColumn("policy_id", F.trim(F.col("policy_id")))
            .withColumn("date_of_loss", F.to_date(F.col("date_of_loss")))
            .withColumn(
                "year_of_loss",
                # Prefer the explicit year_of_loss field; fall back to
                # extracting the year from date_of_loss if year_of_loss is null
                F.coalesce(F.col("year_of_loss").cast(IntegerType()), F.year(F.col("date_of_loss")))
            )
            .withColumn("state", F.upper(F.trim(F.col("state"))))
            .withColumn("reported_city", F.trim(F.col("reported_city")))
            .withColumn("reported_zip_code", F.trim(F.col("reported_zip_code")))
            .withColumn("cause_of_damage", F.trim(F.col("cause_of_damage")))
            .withColumn("amount_paid_building", F.col("amount_paid_building").cast(DecimalType(18, 2)))
            .withColumn("amount_paid_contents", F.col("amount_paid_contents").cast(DecimalType(18, 2)))
            .withColumn("building_damage_amount", F.col("building_damage_amount").cast(DecimalType(18, 2)))
            .withColumn("contents_damage_amount", F.col("contents_damage_amount").cast(DecimalType(18, 2)))
            .withColumn("claim_status", F.trim(F.col("claim_status")))
        )

        canonical_cols = [
            "claim_id",
            "policy_id",
            "date_of_loss",
            "year_of_loss",
            "state",
            "reported_city",
            "reported_zip_code",
            "cause_of_damage",
            "amount_paid_building",
            "amount_paid_contents",
            "building_damage_amount",
            "contents_damage_amount",
            "claim_status",
            "_ingestion_timestamp",
        ]
        df = self._select_available(df, canonical_cols)
        # Deduplicate by claim_id — one record per claim
        df = deduplicate(df, key_columns=["claim_id"])

        dq = DataQualityChecker(df)
        df = (
            dq.check_not_null("claim_id")
            .check_not_null("date_of_loss", "missing_loss_date")
            .check_positive("amount_paid_building")
            .apply()
        )

        write_delta_table(df, CATALOG, SILVER, "silver_property_claims", mode="overwrite")
        print(f"    Rows: {df.count():,}")

    def _safe_union(self, *table_names):
        """Union multiple tables, skipping those that don't exist.

        TRAINEE NOTE — Why a "safe" union?
        In early development or partial pipeline runs, not all Bronze tables
        may exist yet. A regular df1.union(df2) would fail with a
        AnalysisException if either table is missing. _safe_union() handles
        this gracefully: it loads whichever tables exist and unions them.
        If NONE of the tables exist, it returns None (the caller then skips).

        unionByName (not union):
          union() matches columns by POSITION — dangerous if column order differs.
          unionByName() matches columns by NAME — safe even if column order differs
          between the CSV and JSON sources.

        Common column intersection:
          If CSV has columns [A, B, C] and JSON has [A, B, D], we cannot
          union them directly (different schemas). We find the intersection
          [A, B] and union only those columns. This is a safe fallback;
          in production, source schemas should be aligned through governance.
        """
        dfs = []
        for t in table_names:
            try:
                dfs.append(self.spark.table(t))
            except Exception:
                pass  # Table doesn't exist — skip it silently
        if not dfs:
            return None
        result = dfs[0]
        for df in dfs[1:]:
            # Union by name to handle different column sets between CSV and JSON
            common_cols = list(set(result.columns) & set(df.columns))
            if common_cols:
                result = result.select(common_cols).unionByName(df.select(common_cols))
        return result

    def _select_available(self, df, columns):
        """Select only columns that exist in the DataFrame.

        TRAINEE NOTE:
        This defensive helper prevents errors when a source does not contain
        every column in our canonical list. Instead of raising a ColumnNotFound
        error, we simply skip the missing columns and select what is available.

        This is especially important when unioning multiple source types
        (CSV + JSON + API) that may have slightly different column sets.
        The Gold layer handles any still-missing columns by filling with nulls.
        """
        available = [c for c in columns if c in df.columns]
        return df.select(available)
