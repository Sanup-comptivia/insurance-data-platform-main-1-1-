"""
Gold Layer — dim_claim dimension table.
Central claim dimension joining all LOB claims.
PK: claim_key (surrogate), NK: claim_id
FK: policy_key, date_of_loss_key, date_reported_key
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import generate_surrogate_key, deduplicate, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "dim_claim"


def build_dim_claim(spark: SparkSession):
    """Build claim dimension from all Silver LOB claim tables."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    # Load policy dimension for FK lookup
    try:
        policy_df = spark.table(f"{CATALOG}.{GOLD}.dim_policy").select("policy_key", "policy_id")
    except Exception:
        policy_df = None

    frames = []

    # Property claims
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_claims").select(
            F.col("claim_id"),
            F.col("claim_id").alias("claim_number"),
            F.col("policy_id"),
            F.lit(None).cast("string").alias("claimant_name"),
            F.lit("Policyholder").alias("claimant_type"),
            F.col("date_of_loss"),
            F.col("date_of_loss").alias("date_reported"),  # Proxy
            F.col("cause_of_damage").alias("cause_of_loss"),
            F.lit("Property").alias("claim_type"),
            F.col("claim_status"),
            F.lit(False).alias("litigation_flag"),
            F.lit(None).cast("string").alias("catastrophe_code"),
            F.lit(None).cast("string").alias("adjuster_name"),
        )
        frames.append(prop)
    except Exception:
        pass

    # Workers' Comp claims
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_claims").select(
            F.col("claim_id"),
            F.col("claim_id").alias("claim_number"),
            F.col("policy_id"),
            F.lit(None).cast("string").alias("claimant_name"),
            F.lit("Employee").alias("claimant_type"),
            F.col("date_of_injury").alias("date_of_loss"),
            F.col("date_of_injury").alias("date_reported"),
            F.col("cause_of_injury").alias("cause_of_loss"),
            F.col("injury_type").alias("claim_type"),
            F.col("claim_status"),
            F.lit(False).alias("litigation_flag"),
            F.lit(None).cast("string").alias("catastrophe_code"),
            F.lit(None).cast("string").alias("adjuster_name"),
        )
        frames.append(wc)
    except Exception:
        pass

    # Auto claims
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_claims").select(
            F.col("claim_id"),
            F.col("claim_id").alias("claim_number"),
            F.col("policy_id"),
            F.lit(None).cast("string").alias("claimant_name"),
            F.lit("Driver").alias("claimant_type"),
            F.col("date_of_accident").alias("date_of_loss"),
            F.col("date_of_accident").alias("date_reported"),
            F.col("accident_description").alias("cause_of_loss"),
            F.lit("Auto").alias("claim_type"),
            F.col("claim_status"),
            F.lit(False).alias("litigation_flag"),
            F.lit(None).cast("string").alias("catastrophe_code"),
            F.lit(None).cast("string").alias("adjuster_name"),
        )
        frames.append(auto)
    except Exception:
        pass

    # GL claims
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_claims").select(
            F.col("claim_id"),
            F.col("claim_id").alias("claim_number"),
            F.col("policy_id"),
            F.col("claimant_name"),
            F.lit("ThirdParty").alias("claimant_type"),
            F.col("date_of_occurrence").alias("date_of_loss"),
            F.col("date_of_occurrence").alias("date_reported"),
            F.col("claim_description").alias("cause_of_loss"),
            F.col("claim_type"),
            F.col("claim_status"),
            F.col("litigation_flag"),
            F.lit(None).cast("string").alias("catastrophe_code"),
            F.lit(None).cast("string").alias("adjuster_name"),
        )
        frames.append(gl)
    except Exception:
        pass

    # Umbrella claims
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_claims").select(
            F.col("claim_id"),
            F.col("claim_id").alias("claim_number"),
            F.col("policy_id"),
            F.lit(None).cast("string").alias("claimant_name"),
            F.lit("Excess").alias("claimant_type"),
            F.col("date_of_occurrence").alias("date_of_loss"),
            F.col("date_of_occurrence").alias("date_reported"),
            F.col("underlying_line").alias("cause_of_loss"),
            F.lit("Umbrella").alias("claim_type"),
            F.col("claim_status"),
            F.lit(False).alias("litigation_flag"),
            F.lit(None).cast("string").alias("catastrophe_code"),
            F.lit(None).cast("string").alias("adjuster_name"),
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

    df = deduplicate(df, key_columns=["claim_id"], order_column="date_of_loss", ascending=False)

    # Join policy key
    if policy_df is not None:
        df = df.join(
            policy_df.withColumnRenamed("policy_id", "_pol_id"), df["policy_id"] == F.col("_pol_id"), "left"
        ).drop("_pol_id", "policy_id")
    else:
        df = df.withColumn("policy_key", F.lit(None).cast("string")).drop("policy_id")

    # Date keys
    df = (
        df.withColumn(
            "date_of_loss_key",
            F.when(F.col("date_of_loss").isNotNull(), F.date_format(F.col("date_of_loss"), "yyyyMMdd").cast("int")),
        )
        .withColumn(
            "date_reported_key",
            F.when(F.col("date_reported").isNotNull(), F.date_format(F.col("date_reported"), "yyyyMMdd").cast("int")),
        )
        .drop("date_of_loss", "date_reported")
    )

    df = generate_surrogate_key(df, "claim_id", key_name="claim_key")

    df = df.select(
        "claim_key",
        "claim_id",
        "claim_number",
        "policy_key",
        "claimant_name",
        "claimant_type",
        "date_of_loss_key",
        "date_reported_key",
        "cause_of_loss",
        "claim_type",
        "claim_status",
        "litigation_flag",
        "catastrophe_code",
        "adjuster_name",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
