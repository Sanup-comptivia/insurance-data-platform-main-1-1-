"""
Gold Layer — dim_agent dimension table.
Extracts unique agents from all Silver policy tables.
PK: agent_key (surrogate), NK: agent_id
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import generate_surrogate_key, deduplicate, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "dim_agent"


def build_dim_agent(spark: SparkSession):
    """Build agent dimension from Silver policy data."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    silver_tables = [
        ("silver_property_policies", "property_state"),
        ("silver_workers_comp_policies", "state_code"),
        ("silver_auto_policies", "state"),
        ("silver_general_liability_policies", "state_code"),
        ("silver_umbrella_policies", "state_code"),
    ]

    frames = []
    for table, state_col in silver_tables:
        try:
            df = (
                spark.table(f"{CATALOG}.{SILVER}.{table}")
                .select(
                    F.col("agent_id"),
                    F.col("agent_name"),
                    F.col(state_col).alias("state_code"),
                )
                .filter(F.col("agent_id").isNotNull())
            )
            frames.append(df)
        except Exception:
            pass

    if not frames:
        print("    WARN: No Silver data found. Skipping.")
        return None

    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)

    df = deduplicate(df, key_columns=["agent_id"], order_column="agent_name", ascending=True)

    # Add derived columns
    df = (
        df.withColumn("agency_name", F.concat(F.lit("Agency_"), F.substring("agent_id", 5, 6)))
        .withColumn("agency_code", F.substring("agent_id", 5, 6))
        .withColumn("license_number", F.concat(F.lit("LIC-"), F.col("agent_id")))
        .withColumn(
            "commission_tier",
            F.element_at(
                F.array(F.lit("Standard"), F.lit("Preferred"), F.lit("Elite")), (F.abs(F.hash("agent_id")) % 3) + 1
            ),
        )
    )

    df = generate_surrogate_key(df, "agent_id", key_name="agent_key")

    df = df.select(
        "agent_key",
        "agent_id",
        "agent_name",
        "agency_name",
        "agency_code",
        "license_number",
        "state_code",
        "commission_tier",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
