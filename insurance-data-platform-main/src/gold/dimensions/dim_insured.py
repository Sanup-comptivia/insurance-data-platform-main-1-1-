"""
Gold Layer — dim_insured dimension table.
Extracts unique insured entities from all Silver policy tables.
PK: insured_key (surrogate), NK: insured_id
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import generate_surrogate_key, deduplicate, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "dim_insured"


def build_dim_insured(spark: SparkSession):
    """Build insured dimension by extracting unique insured entities across all LOBs."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    frames = []

    # Property — individual policyholders
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.col("policy_id").alias("insured_id"),
            F.concat(F.lit("Insured_"), F.col("policy_id")).alias("insured_name"),
            F.lit("Individual").alias("insured_type"),
            F.lit(None).cast("date").alias("date_of_birth"),
            F.lit(None).cast("string").alias("gender"),
            F.lit(None).cast("string").alias("ein"),
            F.lit(None).cast("string").alias("sic_code"),
            F.lit(None).cast("string").alias("naics_code"),
            F.lit(None).cast("string").alias("industry"),
            F.lit(None).cast("int").alias("employee_count"),
            F.lit(None).cast(DecimalType(18, 2)).alias("annual_revenue"),
        )
        frames.append(prop)
    except Exception:
        pass

    # Workers' Comp — employers
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_policies").select(
            F.col("policy_id").alias("insured_id"),
            F.col("employer_name").alias("insured_name"),
            F.lit("Commercial").alias("insured_type"),
            F.lit(None).cast("date").alias("date_of_birth"),
            F.lit(None).cast("string").alias("gender"),
            F.col("fein").alias("ein"),
            F.lit(None).cast("string").alias("sic_code"),
            F.col("naics_code"),
            F.lit(None).cast("string").alias("industry"),
            F.lit(None).cast("int").alias("employee_count"),
            F.lit(None).cast(DecimalType(18, 2)).alias("annual_revenue"),
        )
        frames.append(wc)
    except Exception:
        pass

    # Auto — individual drivers
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("policy_id").alias("insured_id"),
            F.col("insured_name"),
            F.lit("Individual").alias("insured_type"),
            F.col("insured_dob").alias("date_of_birth"),
            F.col("insured_gender").alias("gender"),
            F.lit(None).cast("string").alias("ein"),
            F.lit(None).cast("string").alias("sic_code"),
            F.lit(None).cast("string").alias("naics_code"),
            F.lit(None).cast("string").alias("industry"),
            F.lit(None).cast("int").alias("employee_count"),
            F.lit(None).cast(DecimalType(18, 2)).alias("annual_revenue"),
        )
        frames.append(auto)
    except Exception:
        pass

    # General Liability — businesses
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("policy_id").alias("insured_id"),
            F.col("insured_name"),
            F.lit("Commercial").alias("insured_type"),
            F.lit(None).cast("date").alias("date_of_birth"),
            F.lit(None).cast("string").alias("gender"),
            F.lit(None).cast("string").alias("ein"),
            F.lit(None).cast("string").alias("sic_code"),
            F.col("naics_code"),
            F.col("business_type").alias("industry"),
            F.lit(None).cast("int").alias("employee_count"),
            F.col("revenue").alias("annual_revenue"),
        )
        frames.append(gl)
    except Exception:
        pass

    # Umbrella
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.col("policy_id").alias("insured_id"),
            F.col("insured_name"),
            F.col("insured_type"),
            F.lit(None).cast("date").alias("date_of_birth"),
            F.lit(None).cast("string").alias("gender"),
            F.lit(None).cast("string").alias("ein"),
            F.lit(None).cast("string").alias("sic_code"),
            F.lit(None).cast("string").alias("naics_code"),
            F.lit(None).cast("string").alias("industry"),
            F.lit(None).cast("int").alias("employee_count"),
            F.lit(None).cast(DecimalType(18, 2)).alias("annual_revenue"),
        )
        frames.append(umb)
    except Exception:
        pass

    if not frames:
        print("    WARN: No Silver data found. Skipping.")
        return None

    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)

    df = deduplicate(df, key_columns=["insured_id"], order_column="insured_name", ascending=True)
    df = generate_surrogate_key(df, "insured_id", key_name="insured_key")

    df = df.select(
        "insured_key",
        "insured_id",
        "insured_name",
        "insured_type",
        "date_of_birth",
        "gender",
        "ein",
        "sic_code",
        "naics_code",
        "industry",
        "employee_count",
        "annual_revenue",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
