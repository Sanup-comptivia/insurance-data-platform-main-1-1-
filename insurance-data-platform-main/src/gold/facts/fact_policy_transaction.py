"""
Gold Layer — fact_policy_transaction fact table.
Grain: one row per policy lifecycle event (new business, renewal, endorsement, cancellation).
FK: policy_key, date_key, lob_key
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import generate_surrogate_key, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "fact_policy_transaction"


def build_fact_policy_transaction(spark: SparkSession):
    """Build policy transaction fact from Silver policy data across all LOBs."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    policy_dim = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_policy", ["policy_key", "policy_id", "lob_key"])

    frames = []

    # Property
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.col("policy_id"),
            F.lit("PROP").alias("lob_code"),
            F.lit("new_business").alias("transaction_type"),
            F.col("premium_amount").alias("premium_change"),
            F.col("effective_date"),
            F.col("effective_date").alias("processed_date"),
        )
        frames.append(prop)
    except Exception:
        pass

    # Workers' Comp
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_policies").select(
            F.col("policy_id"),
            F.lit("WC").alias("lob_code"),
            F.lit("new_business").alias("transaction_type"),
            F.col("premium_amount").alias("premium_change"),
            F.col("effective_date"),
            F.col("effective_date").alias("processed_date"),
        )
        frames.append(wc)
    except Exception:
        pass

    # Auto
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("policy_id"),
            F.lit("AUTO").alias("lob_code"),
            F.lit("new_business").alias("transaction_type"),
            F.col("premium_amount").alias("premium_change"),
            F.col("effective_date"),
            F.col("effective_date").alias("processed_date"),
        )
        frames.append(auto)
    except Exception:
        pass

    # General Liability
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("policy_id"),
            F.lit("GL").alias("lob_code"),
            F.lit("new_business").alias("transaction_type"),
            F.col("premium_amount").alias("premium_change"),
            F.col("effective_date"),
            F.col("effective_date").alias("processed_date"),
        )
        frames.append(gl)
    except Exception:
        pass

    # Umbrella
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.col("policy_id"),
            F.lit("UMB").alias("lob_code"),
            F.lit("new_business").alias("transaction_type"),
            F.col("premium_amount").alias("premium_change"),
            F.col("effective_date"),
            F.col("effective_date").alias("processed_date"),
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

    df = df.withColumn("premium_change", F.col("premium_change").cast(DecimalType(18, 2)))

    # Date key
    df = df.withColumn(
        "date_key",
        F.when(F.col("effective_date").isNotNull(), F.date_format(F.col("effective_date"), "yyyyMMdd").cast("int")),
    )

    # Join policy key and lob_key
    if policy_dim is not None:
        df = df.join(
            policy_dim.select(F.col("policy_key"), F.col("policy_id").alias("_pid"), F.col("lob_key")),
            df["policy_id"] == F.col("_pid"),
            "left",
        ).drop("_pid")
    else:
        df = df.withColumn("policy_key", F.lit(None).cast("string"))
        df = df.withColumn("lob_key", F.lit(None).cast("string"))

    df = generate_surrogate_key(df, "policy_id", "transaction_type", "effective_date", key_name="policy_txn_key")

    df = df.select(
        "policy_txn_key",
        "policy_key",
        "date_key",
        "lob_key",
        "transaction_type",
        "premium_change",
        "effective_date",
        "processed_date",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df


def _safe_table(spark, table_name, cols=None):
    try:
        df = spark.table(table_name)
        return df.select(cols) if cols else df
    except Exception:
        return None
