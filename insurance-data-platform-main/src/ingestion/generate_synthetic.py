"""
Synthetic Insurance Data Generator.
Dual mode: local (Faker, ~1GB) for dev/testing + Databricks (dbldatagen, 100-200GB) for production.

TRAINEE GUIDE — Why do we need synthetic data?
Real insurance data contains sensitive PII (names, addresses, SSNs, medical records).
We cannot use real customer data for development, testing, or training purposes due to
privacy laws (GDPR, CCPA) and company data governance policies.

Synthetic data solves this by generating REALISTIC but FAKE records:
  - Policy IDs, claim IDs, amounts, dates look and behave like real data
  - No real person's information is used or exposed
  - The pipeline can be developed and tested safely

Two generation modes:
  "local" mode:
    - Uses PySpark's built-in random functions (F.rand, spark.range)
    - Generates ~100K-150K rows per table (~1GB total across all LOBs)
    - Runs on a single laptop or small Databricks cluster in minutes
    - Purpose: developer testing, CI/CD pipeline unit tests

  "databricks" mode:
    - Same PySpark approach but with much larger row counts
    - Generates 50M-150M rows per table (~100-200GB total)
    - Requires a large Databricks cluster (8+ workers) and 1-2 hours
    - Purpose: production-scale performance testing, capacity planning

Both modes use seeded randomness (SEED = 42) for REPRODUCIBILITY:
  - Running the generator twice with the same SEED produces IDENTICAL data
  - This means test results are comparable between runs
  - It allows us to detect regressions: "this query returned 1000 rows before,
    now it returns 999 — something in the pipeline changed"

Data is written in BOTH CSV and JSON formats for each LOB because:
  - Different Bronze ingestion paths test different reading code paths
  - Realistic: in enterprise environments, the same data often arrives in
    multiple formats from different source systems simultaneously

Usage (Databricks):
    from src.ingestion.generate_synthetic import SyntheticDataGenerator
    gen = SyntheticDataGenerator(spark, mode="databricks")
    gen.generate_all()

Usage (Local):
    gen = SyntheticDataGenerator(spark, mode="local")
    gen.generate_all()
"""

import os
import random
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class SyntheticDataGenerator:
    """Generate synthetic insurance data for all 5 LOBs.

    TRAINEE NOTE — Class-level constants:
    Constants defined at the class level (SEED, ROW_COUNTS, US_STATES) are
    shared across all instances. Any SyntheticDataGenerator object can access
    them via self.SEED, self.ROW_COUNTS, etc.

    Keeping them at class level (rather than inside __init__) makes them easy
    to find and change in one place — you'd never need to search through method
    bodies to find where "42" is defined.
    """

    # TRAINEE NOTE — Why SEED = 42?
    # 42 is a popular arbitrary seed choice (a cultural reference to "The
    # Hitchhiker's Guide to the Galaxy"). Any integer would work — the key
    # property is that the SAME seed always produces the SAME random sequence.
    # F.rand(seed) in Spark generates a column of random doubles [0.0, 1.0)
    # where the sequence is deterministic given the same seed value.
    SEED = 42

    # TRAINEE NOTE — ROW_COUNTS dictionary:
    # This is the primary configuration for data volume. Each mode maps to a
    # sub-dictionary of {table_name: row_count}.
    # Notice the MASSIVE scale difference between local and databricks:
    #   property_policies: 100,000 (local) vs 150,000,000 (databricks)
    #   That's a 1,500x difference!
    # The databricks counts represent a realistic large US insurer's book of business.
    # The local counts are just enough to exercise all the pipeline code paths.
    ROW_COUNTS = {
        "local": {
            "property_policies": 100_000,
            "property_claims": 30_000,
            "workers_comp_policies": 80_000,
            "workers_comp_claims": 25_000,
            "auto_policies": 100_000,
            "auto_claims": 35_000,
            "general_liability_policies": 60_000,
            "general_liability_claims": 18_000,
            "umbrella_policies": 30_000,
            "umbrella_claims": 5_000,
        },
        "databricks": {
            "property_policies": 150_000_000,
            "property_claims": 45_000_000,
            "workers_comp_policies": 120_000_000,
            "workers_comp_claims": 36_000_000,
            "auto_policies": 130_000_000,
            "auto_claims": 45_000_000,
            "general_liability_policies": 100_000_000,
            "general_liability_claims": 30_000_000,
            "umbrella_policies": 50_000_000,
            "umbrella_claims": 10_000_000,
        },
    }

    US_STATES = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
    ]

    def __init__(
        self,
        spark: SparkSession,
        mode: str = "local",
        output_base: str = "/Volumes/insurance_catalog/bronze/raw_data",
    ):
        self.spark = spark
        self.mode = mode
        # Cloud storage path where generated files are written.
        # /mnt/insurance-data/raw is a Databricks mount point that maps to a
        # cloud storage bucket (Azure ADLS, AWS S3, or GCS).
        # In local mode, this path maps to the local filesystem.
        self.output_base = os.environ.get("INSURANCE_RAW_BASE_PATH", output_base)
        # Select the row count sub-dictionary for this mode (local or databricks)
        self.row_counts = self.ROW_COUNTS[mode]
        # Seed Python's built-in random module for any Python-level randomness.
        # (Spark's F.rand() uses its own seeding mechanism via the SEED constant.)
        random.seed(self.SEED)

    def generate_all(self):
        """Generate synthetic data for all 5 LOBs in all required formats.

        TRAINEE NOTE — Each LOB generator runs sequentially here.
        In a production pipeline optimised for speed, you might run these
        in parallel using Python threads or Databricks multi-task jobs.
        Sequential execution is simpler and easier to debug during development.
        """
        print(f"Generating synthetic data in '{self.mode}' mode")
        print("=" * 60)

        self.generate_property()
        self.generate_workers_comp()
        self.generate_auto()
        self.generate_general_liability()
        self.generate_umbrella()

        print("\n" + "=" * 60)
        print("Synthetic data generation complete!")

    # =========================================================================
    # PROPERTY INSURANCE
    # =========================================================================
    def generate_property(self):
        """Generate property insurance policies and claims.

        TRAINEE NOTE — The core data generation pattern used in ALL LOB generators:

        Step 1: spark.range(0, n_policies)
          Creates a DataFrame with one column "id" containing integers 0, 1, 2, ..., N-1.
          This is the foundation — every policy/claim is derived from one ID row.
          spark.range() is highly efficient because Spark distributes the range
          across workers with no data shuffling needed.

        Step 2: withColumn chains
          Each .withColumn() derives one data column from the row ID or from
          previously added columns. The derivation techniques used here:

          F.concat(F.lit("PROP-"), F.format_string("%010d", F.col("id")))
            → Builds policy_id like "PROP-0000000001", "PROP-0000000002", ...
            → F.lit("PROP-"): a literal string constant
            → F.format_string("%010d", id): zero-pad the integer to 10 digits

          F.rand(SEED) * range + offset
            → Generates a uniformly distributed random decimal in [offset, offset+range]
            → .cast("int") truncates to integer
            → Example: F.rand(42) * 3650 + 0 → random integer 0-3649 (days in 10 years)

          F.element_at(F.array(...), index)
            → Picks one element from a hard-coded array by index (1-based in Spark)
            → Used for categorical columns: states, construction types, claim statuses
            → Example: element_at(["Open","Closed","Pending"], rand*3+1) → random status

          F.date_add(F.lit("2015-01-01"), days)
            → Adds a random number of days to the base date to create realistic dates
            → Policies span 2015-2025 (3650 days from 2015-01-01)

        Step 3: .drop("id")
          Remove the helper "id" column — it was only needed to derive other columns.

        Step 4: Write BOTH CSV and JSON
          The same DataFrame is written twice (different formats, different paths).
          This simulates having the same logical data arrive from two different
          source systems with different output formats.
        """
        print("\n[1/5] Generating Property Insurance data...")

        n_policies = self.row_counts["property_policies"]
        n_claims = self.row_counts["property_claims"]

        # -- Policies (CSV and JSON format) --
        # TRAINEE NOTE — Building the policy DataFrame column by column.
        # Each withColumn adds one field. The SEED offsets (+1, +2, etc.) ensure
        # different columns get DIFFERENT random sequences. Without offsets,
        # two columns using F.rand(42) would produce identical values.
        policies_df = (
            self.spark.range(0, n_policies)
            # Zero-padded ID: "PROP-0000000001", "PROP-0000000002", ...
            .withColumn("policy_id", F.concat(F.lit("PROP-"), F.format_string("%010d", F.col("id"))))
            .withColumn("policy_number", F.concat(F.lit("PN"), F.format_string("%012d", F.col("id"))))
            .withColumn("effective_date", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED) * 3650).cast("int")))
            .withColumn("expiration_date", F.date_add(F.col("effective_date"), F.lit(365)))
            .withColumn("insured_name", F.concat(F.lit("Insured_"), F.col("id").cast("string")))
            .withColumn(
                "property_state",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 1) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn(
                "reported_city", F.concat(F.lit("City_"), (F.rand(self.SEED + 2) * 1000).cast("int").cast("string"))
            )
            .withColumn("reported_zip_code", F.format_string("%05d", (F.rand(self.SEED + 3) * 99999).cast("int")))
            .withColumn("latitude", F.round(F.rand(self.SEED + 4) * 25 + 25, 6))
            .withColumn("longitude", F.round(F.rand(self.SEED + 5) * -57 - 67, 6))
            .withColumn(
                "occupancy_type",
                F.element_at(
                    F.array(F.lit("1"), F.lit("2"), F.lit("3"), F.lit("4")), (F.rand(self.SEED + 6) * 4).cast("int") + 1
                ),
            )
            .withColumn(
                "construction_type",
                F.element_at(
                    F.array(F.lit("Frame"), F.lit("Masonry"), F.lit("Steel"), F.lit("Concrete")),
                    (F.rand(self.SEED + 7) * 4).cast("int") + 1,
                ),
            )
            .withColumn("year_built", (F.rand(self.SEED + 8) * 74 + 1950).cast("int"))
            .withColumn("number_of_floors", (F.rand(self.SEED + 9) * 3 + 1).cast("int"))
            .withColumn("building_value", F.round(F.rand(self.SEED + 10) * 900000 + 100000, 2))
            .withColumn("contents_value", F.round(F.rand(self.SEED + 11) * 200000 + 10000, 2))
            .withColumn("total_building_coverage", F.round(F.col("building_value") * 0.8, 2))
            .withColumn("total_contents_coverage", F.round(F.col("contents_value") * 0.7, 2))
            .withColumn(
                "deductible_amount",
                F.element_at(
                    F.array(F.lit(500.0), F.lit(1000.0), F.lit(2500.0), F.lit(5000.0)),
                    (F.rand(self.SEED + 12) * 4).cast("int") + 1,
                ),
            )
            .withColumn("premium_amount", F.round(F.rand(self.SEED + 13) * 4000 + 500, 2))
            .withColumn(
                "flood_zone",
                F.element_at(
                    F.array(F.lit("A"), F.lit("AE"), F.lit("V"), F.lit("X"), F.lit("B"), F.lit("C")),
                    (F.rand(self.SEED + 14) * 6).cast("int") + 1,
                ),
            )
            .withColumn("primary_residence", (F.rand(self.SEED + 15) > 0.3).cast("string"))
            .withColumn(
                "agent_id",
                F.concat(F.lit("AGT-"), F.format_string("%06d", (F.rand(self.SEED + 16) * 5000).cast("int"))),
            )
            .withColumn(
                "agent_name", F.concat(F.lit("Agent_"), (F.rand(self.SEED + 17) * 5000).cast("int").cast("string"))
            )
            .withColumn(
                "underwriter",
                F.element_at(
                    F.array(F.lit("UW_Alpha"), F.lit("UW_Beta"), F.lit("UW_Gamma"), F.lit("UW_Delta")),
                    (F.rand(self.SEED + 18) * 4).cast("int") + 1,
                ),
            )
            .drop("id")
        )

        # Write CSV (structured) — simulates a FEMA-style bulk flat file export
        output_csv = f"{self.output_base}/property/synthetic_policies_csv"
        policies_df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"  -> Property policies CSV: {n_policies} rows")

        # Write JSON (semi-structured) — simulates a REST API response or JSON export
        # TRAINEE NOTE: Writing the SAME data in two formats tests both
        # BronzeStructuredIngestion (CSV) and BronzeSemiStructuredIngestion (JSON).
        output_json = f"{self.output_base}/property/synthetic_policies_json"
        policies_df.write.mode("overwrite").json(output_json)
        print(f"  -> Property policies JSON: {n_policies} rows")

        # -- Claims (CSV and JSON format) --
        # TRAINEE NOTE — Claims reference policies via policy_id (foreign key).
        # The claim's policy_id is chosen randomly from the range of valid policy IDs:
        #   F.rand(SEED+20) * n_policies → random float in [0, n_policies)
        #   .cast("long")               → truncate to integer = a valid policy index
        #   format_string("%010d")      → format to match policy_id convention
        # This creates a realistic many-to-one relationship: multiple claims can
        # reference the same policy (one policy can have multiple claims over time).
        claims_df = (
            self.spark.range(0, n_claims)
            .withColumn("claim_id", F.concat(F.lit("CLM-PROP-"), F.format_string("%010d", F.col("id"))))
            .withColumn(
                "policy_id",
                # Randomly assign this claim to one of the generated policies
                F.concat(F.lit("PROP-"), F.format_string("%010d", (F.rand(self.SEED + 20) * n_policies).cast("long"))),
            )
            .withColumn("date_of_loss", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED + 21) * 3650).cast("int")))
            .withColumn("year_of_loss", F.year(F.col("date_of_loss")))
            .withColumn(
                "state",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 22) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn(
                "cause_of_damage",
                F.element_at(
                    F.array(
                        F.lit("Fire"),
                        F.lit("Wind"),
                        F.lit("Water"),
                        F.lit("Hail"),
                        F.lit("Theft"),
                        F.lit("Vandalism"),
                        F.lit("Flood"),
                        F.lit("Lightning"),
                    ),
                    (F.rand(self.SEED + 23) * 8).cast("int") + 1,
                ),
            )
            .withColumn("amount_paid_building", F.round(F.rand(self.SEED + 24) * 150000, 2))
            .withColumn("amount_paid_contents", F.round(F.rand(self.SEED + 25) * 50000, 2))
            .withColumn(
                "building_damage_amount",
                F.round(F.col("amount_paid_building") * (F.rand(self.SEED + 26) * 0.5 + 1.0), 2),
            )
            .withColumn(
                "contents_damage_amount",
                F.round(F.col("amount_paid_contents") * (F.rand(self.SEED + 27) * 0.5 + 1.0), 2),
            )
            .withColumn(
                "claim_status",
                F.element_at(
                    F.array(F.lit("Open"), F.lit("Closed"), F.lit("Pending"), F.lit("Reopened")),
                    (F.rand(self.SEED + 28) * 4).cast("int") + 1,
                ),
            )
            .withColumn(
                "reported_city", F.concat(F.lit("City_"), (F.rand(self.SEED + 29) * 1000).cast("int").cast("string"))
            )
            .withColumn("reported_zip_code", F.format_string("%05d", (F.rand(self.SEED + 30) * 99999).cast("int")))
            .drop("id")
        )

        output_claims_csv = f"{self.output_base}/property/synthetic_claims_csv"
        claims_df.write.mode("overwrite").option("header", "true").csv(output_claims_csv)
        print(f"  -> Property claims CSV: {n_claims} rows")

        # Write JSON (semi-structured)
        output_claims_json = f"{self.output_base}/property/synthetic_claims_json"
        claims_df.write.mode("overwrite").json(output_claims_json)
        print(f"  -> Property claims JSON: {n_claims} rows")

    # =========================================================================
    # WORKERS' COMPENSATION
    # =========================================================================
    def generate_workers_comp(self):
        """Generate workers' compensation policies and claims.

        TRAINEE NOTE — WC-specific generation choices:

        class_codes: NCCI (National Council on Compensation Insurance) class codes
          classify the type of work performed by employees. Each code has an
          associated base rate. Common codes:
            8810 = Clerical (low risk)
            8742 = Outside salespersons (medium risk)
            5403 = Carpentry (high risk)
            9014 = Buildings / property operations (medium risk)

        experience_mod_factor: F.rand(SEED+7) * 1.5 + 0.5
          Range: [0.5, 2.0] — a realistic range for experience modification.
          Values < 1.0 mean the employer has a BETTER-than-average claim history.
          Values > 1.0 mean the employer has a WORSE-than-average claim history.
          Most employers fall between 0.7 and 1.3.

        premium_amount: payroll * experience_mod * 0.02
          WC premium = payroll × class_rate × experience_mod
          Here we use 0.02 (2%) as a simplified flat class rate.
          In reality, class rates vary from ~0.3% (clerical) to ~15% (logging).

        injury_types, body_parts, causes: Realistic WC claim categorizations
          used by claims adjusters and NCCI for statistical reporting.
        """
        print("\n[2/5] Generating Workers' Compensation data...")

        n_policies = self.row_counts["workers_comp_policies"]
        n_claims = self.row_counts["workers_comp_claims"]

        injury_types = ["Strain", "Sprain", "Fracture", "Laceration", "Contusion", "Burn", "Concussion", "Amputation"]
        body_parts = ["Back", "Shoulder", "Knee", "Hand", "Wrist", "Head", "Neck", "Ankle", "Foot", "Elbow"]
        causes = [
            "Lifting",
            "Fall",
            "Struck_By",
            "Caught_In",
            "Overexertion",
            "Slip",
            "Vehicle_Accident",
            "Repetitive_Motion",
        ]
        class_codes = ["8810", "8742", "5191", "5183", "8017", "7380", "8832", "5022", "5403", "9014"]

        policies_df = (
            self.spark.range(0, n_policies)
            .withColumn("policy_id", F.concat(F.lit("WC-"), F.format_string("%010d", F.col("id"))))
            .withColumn("policy_number", F.concat(F.lit("WC"), F.format_string("%012d", F.col("id"))))
            .withColumn("effective_date", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED) * 3650).cast("int")))
            .withColumn("expiration_date", F.date_add(F.col("effective_date"), F.lit(365)))
            .withColumn("employer_name", F.concat(F.lit("Employer_"), F.col("id").cast("string")))
            .withColumn("insured_name", F.col("employer_name"))
            .withColumn(
                "fein",
                F.format_string(
                    "%02d-%07d", (F.rand(self.SEED + 1) * 99).cast("int"), (F.rand(self.SEED + 2) * 9999999).cast("int")
                ),
            )
            .withColumn("naics_code", F.format_string("%06d", (F.rand(self.SEED + 3) * 999999).cast("int")))
            .withColumn(
                "class_code",
                F.element_at(
                    F.array(*[F.lit(c) for c in class_codes]),
                    (F.rand(self.SEED + 4) * len(class_codes)).cast("int") + 1,
                ),
            )
            .withColumn(
                "state_code",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 5) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn("payroll_amount", F.round(F.rand(self.SEED + 6) * 5000000 + 100000, 2))
            .withColumn("experience_mod_factor", F.round(F.rand(self.SEED + 7) * 1.5 + 0.5, 3))
            .withColumn("premium_amount", F.round(F.col("payroll_amount") * F.col("experience_mod_factor") * 0.02, 2))
            .withColumn(
                "agent_id", F.concat(F.lit("AGT-"), F.format_string("%06d", (F.rand(self.SEED + 8) * 5000).cast("int")))
            )
            .withColumn(
                "agent_name", F.concat(F.lit("Agent_"), (F.rand(self.SEED + 9) * 5000).cast("int").cast("string"))
            )
            .withColumn(
                "underwriter",
                F.element_at(
                    F.array(F.lit("UW_Alpha"), F.lit("UW_Beta"), F.lit("UW_Gamma")),
                    (F.rand(self.SEED + 10) * 3).cast("int") + 1,
                ),
            )
            .drop("id")
        )

        output_csv = f"{self.output_base}/workers_comp/synthetic_policies_csv"
        policies_df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"  -> WC policies CSV: {n_policies} rows")

        # Claims — JSON format (semi-structured)
        claims_df = (
            self.spark.range(0, n_claims)
            .withColumn("claim_id", F.concat(F.lit("CLM-WC-"), F.format_string("%010d", F.col("id"))))
            .withColumn(
                "policy_id",
                F.concat(F.lit("WC-"), F.format_string("%010d", (F.rand(self.SEED + 20) * n_policies).cast("long"))),
            )
            .withColumn("date_of_injury", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED + 21) * 3650).cast("int")))
            .withColumn(
                "injury_type",
                F.element_at(
                    F.array(*[F.lit(i) for i in injury_types]),
                    (F.rand(self.SEED + 22) * len(injury_types)).cast("int") + 1,
                ),
            )
            .withColumn(
                "body_part",
                F.element_at(
                    F.array(*[F.lit(b) for b in body_parts]), (F.rand(self.SEED + 23) * len(body_parts)).cast("int") + 1
                ),
            )
            .withColumn("nature_of_injury", F.col("injury_type"))
            .withColumn(
                "cause_of_injury",
                F.element_at(
                    F.array(*[F.lit(c) for c in causes]), (F.rand(self.SEED + 24) * len(causes)).cast("int") + 1
                ),
            )
            .withColumn("employee_age", (F.rand(self.SEED + 25) * 45 + 18).cast("int"))
            .withColumn(
                "employee_gender",
                F.element_at(F.array(F.lit("M"), F.lit("F")), (F.rand(self.SEED + 26) * 2).cast("int") + 1),
            )
            .withColumn("medical_paid", F.round(F.rand(self.SEED + 27) * 80000, 2))
            .withColumn("indemnity_paid", F.round(F.rand(self.SEED + 28) * 120000, 2))
            .withColumn("total_incurred", F.round(F.col("medical_paid") + F.col("indemnity_paid"), 2))
            .withColumn(
                "claim_status",
                F.element_at(
                    F.array(F.lit("Open"), F.lit("Closed"), F.lit("Pending"), F.lit("Reopened")),
                    (F.rand(self.SEED + 29) * 4).cast("int") + 1,
                ),
            )
            .withColumn(
                "return_to_work_date",
                F.when(
                    F.col("claim_status") == "Closed",
                    F.date_add(F.col("date_of_injury"), (F.rand(self.SEED + 30) * 180).cast("int")),
                ),
            )
            .drop("id")
        )

        output_json = f"{self.output_base}/workers_comp/synthetic_claims_json"
        claims_df.write.mode("overwrite").json(output_json)
        print(f"  -> WC claims JSON: {n_claims} rows")

    # =========================================================================
    # AUTO INSURANCE
    # =========================================================================
    def generate_auto(self):
        """Generate auto insurance policies and claims."""
        print("\n[3/5] Generating Auto Insurance data...")

        n_policies = self.row_counts["auto_policies"]
        n_claims = self.row_counts["auto_claims"]

        makes = ["Toyota", "Honda", "Ford", "Chevrolet", "BMW", "Mercedes", "Nissan", "Hyundai", "Kia", "Tesla"]
        models = ["Sedan", "SUV", "Truck", "Coupe", "Van", "Hatchback", "Wagon", "Convertible"]
        coverage_types = ["Comprehensive", "Collision", "Liability", "Full_Coverage", "Minimum"]

        policies_df = (
            self.spark.range(0, n_policies)
            .withColumn("policy_id", F.concat(F.lit("AUTO-"), F.format_string("%010d", F.col("id"))))
            .withColumn("policy_number", F.concat(F.lit("AU"), F.format_string("%012d", F.col("id"))))
            .withColumn("effective_date", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED) * 3650).cast("int")))
            .withColumn("expiration_date", F.date_add(F.col("effective_date"), F.lit(180)))
            .withColumn("insured_name", F.concat(F.lit("Driver_"), F.col("id").cast("string")))
            .withColumn("insured_dob", F.date_add(F.lit("1950-01-01"), (F.rand(self.SEED + 1) * 18250).cast("int")))
            .withColumn(
                "insured_gender",
                F.element_at(F.array(F.lit("M"), F.lit("F")), (F.rand(self.SEED + 2) * 2).cast("int") + 1),
            )
            .withColumn("vehicle_year", (F.rand(self.SEED + 3) * 25 + 2000).cast("int"))
            .withColumn(
                "vehicle_make",
                F.element_at(F.array(*[F.lit(m) for m in makes]), (F.rand(self.SEED + 4) * len(makes)).cast("int") + 1),
            )
            .withColumn(
                "vehicle_model",
                F.element_at(
                    F.array(*[F.lit(m) for m in models]), (F.rand(self.SEED + 5) * len(models)).cast("int") + 1
                ),
            )
            .withColumn(
                "vin",
                F.concat(F.lit("1HGBH41JXMN"), F.format_string("%06d", (F.rand(self.SEED + 6) * 999999).cast("int"))),
            )
            .withColumn(
                "garage_state",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 7) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn("garage_zip", F.format_string("%05d", (F.rand(self.SEED + 8) * 99999).cast("int")))
            .withColumn(
                "coverage_type",
                F.element_at(
                    F.array(*[F.lit(c) for c in coverage_types]),
                    (F.rand(self.SEED + 9) * len(coverage_types)).cast("int") + 1,
                ),
            )
            .withColumn(
                "liability_limit",
                F.element_at(
                    F.array(F.lit(25000.0), F.lit(50000.0), F.lit(100000.0), F.lit(250000.0), F.lit(500000.0)),
                    (F.rand(self.SEED + 10) * 5).cast("int") + 1,
                ),
            )
            .withColumn(
                "deductible_amount",
                F.element_at(
                    F.array(F.lit(250.0), F.lit(500.0), F.lit(1000.0), F.lit(2000.0)),
                    (F.rand(self.SEED + 11) * 4).cast("int") + 1,
                ),
            )
            .withColumn("premium_amount", F.round(F.rand(self.SEED + 12) * 3000 + 600, 2))
            .withColumn(
                "agent_id",
                F.concat(F.lit("AGT-"), F.format_string("%06d", (F.rand(self.SEED + 13) * 5000).cast("int"))),
            )
            .withColumn(
                "agent_name", F.concat(F.lit("Agent_"), (F.rand(self.SEED + 14) * 5000).cast("int").cast("string"))
            )
            .withColumn(
                "underwriter",
                F.element_at(
                    F.array(F.lit("UW_Alpha"), F.lit("UW_Beta"), F.lit("UW_Gamma")),
                    (F.rand(self.SEED + 15) * 3).cast("int") + 1,
                ),
            )
            .withColumn(
                "address", F.concat((F.rand(self.SEED + 16) * 9999).cast("int").cast("string"), F.lit(" Main St"))
            )
            .withColumn("city", F.concat(F.lit("City_"), (F.rand(self.SEED + 17) * 1000).cast("int").cast("string")))
            .withColumn("state", F.col("garage_state"))
            .withColumn("zip_code", F.col("garage_zip"))
            .drop("id")
        )

        output_csv = f"{self.output_base}/auto/synthetic_policies_csv"
        policies_df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"  -> Auto policies CSV: {n_policies} rows")

        output_json = f"{self.output_base}/auto/synthetic_policies_json"
        policies_df.write.mode("overwrite").json(output_json)
        print(f"  -> Auto policies JSON: {n_policies} rows")

        # Claims
        claims_df = (
            self.spark.range(0, n_claims)
            .withColumn("claim_id", F.concat(F.lit("CLM-AUTO-"), F.format_string("%010d", F.col("id"))))
            .withColumn(
                "policy_id",
                F.concat(F.lit("AUTO-"), F.format_string("%010d", (F.rand(self.SEED + 20) * n_policies).cast("long"))),
            )
            .withColumn(
                "date_of_accident", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED + 21) * 3650).cast("int"))
            )
            .withColumn(
                "accident_description",
                F.element_at(
                    F.array(
                        F.lit("Rear-end collision"),
                        F.lit("Side-impact collision"),
                        F.lit("Single vehicle accident"),
                        F.lit("Multi-vehicle pileup"),
                        F.lit("Parking lot incident"),
                        F.lit("Hit and run"),
                    ),
                    (F.rand(self.SEED + 22) * 6).cast("int") + 1,
                ),
            )
            .withColumn(
                "fault_indicator",
                F.element_at(
                    F.array(F.lit("At_Fault"), F.lit("Not_At_Fault"), F.lit("Shared")),
                    (F.rand(self.SEED + 23) * 3).cast("int") + 1,
                ),
            )
            .withColumn("number_vehicles_involved", (F.rand(self.SEED + 24) * 3 + 1).cast("int"))
            .withColumn("bodily_injury_count", (F.rand(self.SEED + 25) * 4).cast("int"))
            .withColumn("property_damage_amount", F.round(F.rand(self.SEED + 26) * 50000, 2))
            .withColumn(
                "total_claim_amount", F.round(F.col("property_damage_amount") + F.rand(self.SEED + 27) * 100000, 2)
            )
            .withColumn(
                "claim_status",
                F.element_at(
                    F.array(F.lit("Open"), F.lit("Closed"), F.lit("Pending"), F.lit("Reopened")),
                    (F.rand(self.SEED + 28) * 4).cast("int") + 1,
                ),
            )
            .drop("id")
        )

        output_claims_csv = f"{self.output_base}/auto/synthetic_claims_csv"
        claims_df.write.mode("overwrite").option("header", "true").csv(output_claims_csv)
        print(f"  -> Auto claims CSV: {n_claims} rows")

    # =========================================================================
    # GENERAL LIABILITY
    # =========================================================================
    def generate_general_liability(self):
        """Generate general liability policies and claims."""
        print("\n[4/5] Generating General Liability data...")

        n_policies = self.row_counts["general_liability_policies"]
        n_claims = self.row_counts["general_liability_claims"]

        business_types = [
            "Restaurant",
            "Retail",
            "Manufacturing",
            "Construction",
            "Healthcare",
            "Technology",
            "Hospitality",
            "Professional_Services",
        ]
        claim_types = ["BodilyInjury", "PropertyDamage", "PersonalInjury", "Advertising", "ProductLiability"]

        policies_df = (
            self.spark.range(0, n_policies)
            .withColumn("policy_id", F.concat(F.lit("GL-"), F.format_string("%010d", F.col("id"))))
            .withColumn("policy_number", F.concat(F.lit("GL"), F.format_string("%012d", F.col("id"))))
            .withColumn("effective_date", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED) * 3650).cast("int")))
            .withColumn("expiration_date", F.date_add(F.col("effective_date"), F.lit(365)))
            .withColumn("insured_name", F.concat(F.lit("Business_"), F.col("id").cast("string")))
            .withColumn(
                "business_type",
                F.element_at(
                    F.array(*[F.lit(b) for b in business_types]),
                    (F.rand(self.SEED + 1) * len(business_types)).cast("int") + 1,
                ),
            )
            .withColumn("naics_code", F.format_string("%06d", (F.rand(self.SEED + 2) * 999999).cast("int")))
            .withColumn("class_code", F.format_string("%05d", (F.rand(self.SEED + 3) * 99999).cast("int")))
            .withColumn(
                "state_code",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 4) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn("revenue", F.round(F.rand(self.SEED + 5) * 10000000 + 100000, 2))
            .withColumn(
                "occurrence_limit",
                F.element_at(
                    F.array(F.lit(500000.0), F.lit(1000000.0), F.lit(2000000.0), F.lit(5000000.0)),
                    (F.rand(self.SEED + 6) * 4).cast("int") + 1,
                ),
            )
            .withColumn("aggregate_limit", F.col("occurrence_limit") * 2)
            .withColumn(
                "deductible_amount",
                F.element_at(
                    F.array(F.lit(1000.0), F.lit(2500.0), F.lit(5000.0), F.lit(10000.0)),
                    (F.rand(self.SEED + 7) * 4).cast("int") + 1,
                ),
            )
            .withColumn("premium_amount", F.round(F.rand(self.SEED + 8) * 15000 + 1000, 2))
            .withColumn(
                "agent_id", F.concat(F.lit("AGT-"), F.format_string("%06d", (F.rand(self.SEED + 9) * 5000).cast("int")))
            )
            .withColumn(
                "agent_name", F.concat(F.lit("Agent_"), (F.rand(self.SEED + 10) * 5000).cast("int").cast("string"))
            )
            .withColumn(
                "underwriter",
                F.element_at(
                    F.array(F.lit("UW_Alpha"), F.lit("UW_Beta"), F.lit("UW_Gamma")),
                    (F.rand(self.SEED + 11) * 3).cast("int") + 1,
                ),
            )
            .withColumn(
                "address", F.concat((F.rand(self.SEED + 12) * 9999).cast("int").cast("string"), F.lit(" Commerce Blvd"))
            )
            .withColumn("city", F.concat(F.lit("City_"), (F.rand(self.SEED + 13) * 1000).cast("int").cast("string")))
            .withColumn("state", F.col("state_code"))
            .withColumn("zip_code", F.format_string("%05d", (F.rand(self.SEED + 14) * 99999).cast("int")))
            .drop("id")
        )

        output_csv = f"{self.output_base}/general_liability/synthetic_policies_csv"
        policies_df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"  -> GL policies CSV: {n_policies} rows")

        # Claims — XML-like structure written as JSON with nested fields
        claims_df = (
            self.spark.range(0, n_claims)
            .withColumn("claim_id", F.concat(F.lit("CLM-GL-"), F.format_string("%010d", F.col("id"))))
            .withColumn(
                "policy_id",
                F.concat(F.lit("GL-"), F.format_string("%010d", (F.rand(self.SEED + 20) * n_policies).cast("long"))),
            )
            .withColumn(
                "date_of_occurrence", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED + 21) * 3650).cast("int"))
            )
            .withColumn(
                "claim_type",
                F.element_at(
                    F.array(*[F.lit(c) for c in claim_types]),
                    (F.rand(self.SEED + 22) * len(claim_types)).cast("int") + 1,
                ),
            )
            .withColumn(
                "claim_description",
                F.concat(F.lit("GL claim for "), F.col("claim_type"), F.lit(" incident #"), F.col("id").cast("string")),
            )
            .withColumn("claimant_name", F.concat(F.lit("Claimant_"), F.col("id").cast("string")))
            .withColumn("incurred_amount", F.round(F.rand(self.SEED + 23) * 500000 + 1000, 2))
            .withColumn("paid_amount", F.round(F.col("incurred_amount") * F.rand(self.SEED + 24), 2))
            .withColumn("reserve_amount", F.round(F.col("incurred_amount") - F.col("paid_amount"), 2))
            .withColumn(
                "claim_status",
                F.element_at(
                    F.array(F.lit("Open"), F.lit("Closed"), F.lit("Pending"), F.lit("Reopened")),
                    (F.rand(self.SEED + 25) * 4).cast("int") + 1,
                ),
            )
            .withColumn("litigation_flag", (F.rand(self.SEED + 26) > 0.8).cast("string"))
            .drop("id")
        )

        output_claims_json = f"{self.output_base}/general_liability/synthetic_claims_json"
        claims_df.write.mode("overwrite").json(output_claims_json)
        print(f"  -> GL claims JSON: {n_claims} rows")

    # =========================================================================
    # UMBRELLA INSURANCE
    # =========================================================================
    def generate_umbrella(self):
        """Generate umbrella insurance policies and claims."""
        print("\n[5/5] Generating Umbrella Insurance data...")

        n_policies = self.row_counts["umbrella_policies"]
        n_claims = self.row_counts["umbrella_claims"]
        n_auto = self.row_counts["auto_policies"]
        n_gl = self.row_counts["general_liability_policies"]
        n_wc = self.row_counts["workers_comp_policies"]

        underlying_lines = ["Auto", "GeneralLiability", "WorkersComp", "EmployersLiability"]

        policies_df = (
            self.spark.range(0, n_policies)
            .withColumn("policy_id", F.concat(F.lit("UMB-"), F.format_string("%010d", F.col("id"))))
            .withColumn("policy_number", F.concat(F.lit("UB"), F.format_string("%012d", F.col("id"))))
            .withColumn("effective_date", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED) * 3650).cast("int")))
            .withColumn("expiration_date", F.date_add(F.col("effective_date"), F.lit(365)))
            .withColumn("insured_name", F.concat(F.lit("Insured_Umb_"), F.col("id").cast("string")))
            .withColumn(
                "insured_type",
                F.element_at(
                    F.array(F.lit("Individual"), F.lit("Commercial")), (F.rand(self.SEED + 1) * 2).cast("int") + 1
                ),
            )
            .withColumn(
                "umbrella_limit",
                F.element_at(
                    F.array(F.lit(1000000.0), F.lit(2000000.0), F.lit(5000000.0), F.lit(10000000.0)),
                    (F.rand(self.SEED + 2) * 4).cast("int") + 1,
                ),
            )
            .withColumn(
                "retention_amount",
                F.element_at(
                    F.array(F.lit(0.0), F.lit(5000.0), F.lit(10000.0)), (F.rand(self.SEED + 3) * 3).cast("int") + 1
                ),
            )
            .withColumn("premium_amount", F.round(F.rand(self.SEED + 4) * 5000 + 200, 2))
            .withColumn(
                "underlying_auto_policy_id",
                F.concat(F.lit("AUTO-"), F.format_string("%010d", (F.rand(self.SEED + 5) * n_auto).cast("long"))),
            )
            .withColumn(
                "underlying_gl_policy_id",
                F.concat(F.lit("GL-"), F.format_string("%010d", (F.rand(self.SEED + 6) * n_gl).cast("long"))),
            )
            .withColumn(
                "underlying_wc_policy_id",
                F.concat(F.lit("WC-"), F.format_string("%010d", (F.rand(self.SEED + 7) * n_wc).cast("long"))),
            )
            .withColumn(
                "agent_id", F.concat(F.lit("AGT-"), F.format_string("%06d", (F.rand(self.SEED + 8) * 5000).cast("int")))
            )
            .withColumn(
                "agent_name", F.concat(F.lit("Agent_"), (F.rand(self.SEED + 9) * 5000).cast("int").cast("string"))
            )
            .withColumn(
                "underwriter",
                F.element_at(
                    F.array(F.lit("UW_Alpha"), F.lit("UW_Beta"), F.lit("UW_Gamma")),
                    (F.rand(self.SEED + 10) * 3).cast("int") + 1,
                ),
            )
            .withColumn(
                "state_code",
                F.element_at(
                    F.array(*[F.lit(s) for s in self.US_STATES]),
                    (F.rand(self.SEED + 11) * len(self.US_STATES)).cast("int") + 1,
                ),
            )
            .withColumn("city", F.concat(F.lit("City_"), (F.rand(self.SEED + 12) * 1000).cast("int").cast("string")))
            .withColumn("zip_code", F.format_string("%05d", (F.rand(self.SEED + 13) * 99999).cast("int")))
            .drop("id")
        )

        output_csv = f"{self.output_base}/umbrella/synthetic_policies_csv"
        policies_df.write.mode("overwrite").option("header", "true").csv(output_csv)
        print(f"  -> Umbrella policies CSV: {n_policies} rows")

        output_json = f"{self.output_base}/umbrella/synthetic_policies_json"
        policies_df.write.mode("overwrite").json(output_json)
        print(f"  -> Umbrella policies JSON: {n_policies} rows")

        # Claims
        claims_df = (
            self.spark.range(0, n_claims)
            .withColumn("claim_id", F.concat(F.lit("CLM-UMB-"), F.format_string("%010d", F.col("id"))))
            .withColumn(
                "policy_id",
                F.concat(F.lit("UMB-"), F.format_string("%010d", (F.rand(self.SEED + 20) * n_policies).cast("long"))),
            )
            .withColumn(
                "date_of_occurrence", F.date_add(F.lit("2015-01-01"), (F.rand(self.SEED + 21) * 3650).cast("int"))
            )
            .withColumn(
                "underlying_line",
                F.element_at(
                    F.array(*[F.lit(u) for u in underlying_lines]),
                    (F.rand(self.SEED + 22) * len(underlying_lines)).cast("int") + 1,
                ),
            )
            .withColumn(
                "underlying_claim_id",
                F.concat(F.lit("CLM-UND-"), F.format_string("%010d", (F.rand(self.SEED + 23) * 1000000).cast("long"))),
            )
            .withColumn("excess_amount", F.round(F.rand(self.SEED + 24) * 500000 + 10000, 2))
            .withColumn("total_claim_amount", F.round(F.col("excess_amount") * (F.rand(self.SEED + 25) + 1.5), 2))
            .withColumn(
                "claim_status",
                F.element_at(
                    F.array(F.lit("Open"), F.lit("Closed"), F.lit("Pending")),
                    (F.rand(self.SEED + 26) * 3).cast("int") + 1,
                ),
            )
            .drop("id")
        )

        output_claims_csv = f"{self.output_base}/umbrella/synthetic_claims_csv"
        claims_df.write.mode("overwrite").option("header", "true").csv(output_claims_csv)
        print(f"  -> Umbrella claims CSV: {n_claims} rows")
