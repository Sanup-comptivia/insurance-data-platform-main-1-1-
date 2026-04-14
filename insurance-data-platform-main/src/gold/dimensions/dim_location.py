"""
Gold Layer — dim_location dimension table.
Extracts unique locations from all Silver policy tables.
PK: location_key (surrogate), NK: location_id (derived from address components)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import generate_surrogate_key, deduplicate, write_delta_table, US_STATES

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "dim_location"


def build_dim_location(spark: SparkSession):
    """Build location dimension from Silver policy data."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    frames = []

    # Property
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.lit(None).cast("string").alias("address_line1"),
            F.lit(None).cast("string").alias("address_line2"),
            F.col("reported_city").alias("city"),
            F.lit(None).cast("string").alias("county"),
            F.col("property_state").alias("state_code"),
            F.col("reported_zip_code").alias("zip_code"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("flood_zone"),
        )
        frames.append(prop)
    except Exception:
        pass

    # Auto
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("address").alias("address_line1"),
            F.lit(None).cast("string").alias("address_line2"),
            F.col("city"),
            F.lit(None).cast("string").alias("county"),
            F.col("garage_state").alias("state_code"),
            F.col("garage_zip").alias("zip_code"),
            F.lit(None).cast("double").alias("latitude"),
            F.lit(None).cast("double").alias("longitude"),
            F.lit(None).cast("string").alias("flood_zone"),
        )
        frames.append(auto)
    except Exception:
        pass

    # General Liability
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("address").alias("address_line1"),
            F.lit(None).cast("string").alias("address_line2"),
            F.col("city"),
            F.lit(None).cast("string").alias("county"),
            F.col("state_code"),
            F.col("zip_code"),
            F.lit(None).cast("double").alias("latitude"),
            F.lit(None).cast("double").alias("longitude"),
            F.lit(None).cast("string").alias("flood_zone"),
        )
        frames.append(gl)
    except Exception:
        pass

    # Umbrella
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.lit(None).cast("string").alias("address_line1"),
            F.lit(None).cast("string").alias("address_line2"),
            F.col("city"),
            F.lit(None).cast("string").alias("county"),
            F.col("state_code"),
            F.col("zip_code"),
            F.lit(None).cast("double").alias("latitude"),
            F.lit(None).cast("double").alias("longitude"),
            F.lit(None).cast("string").alias("flood_zone"),
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

    # Create location_id from key components
    df = df.withColumn(
        "location_id",
        F.md5(
            F.concat_ws(
                "||",
                F.coalesce(F.col("address_line1"), F.lit("")),
                F.coalesce(F.col("city"), F.lit("")),
                F.coalesce(F.col("state_code"), F.lit("")),
                F.coalesce(F.col("zip_code"), F.lit("")),
            )
        ),
    )

    df = deduplicate(df, key_columns=["location_id"], order_column="city", ascending=True)

    # Add state name mapping
    state_mapping = spark.createDataFrame([(k, v) for k, v in US_STATES.items()], ["_state_code", "state_name"])
    df = df.join(state_mapping, df["state_code"] == state_mapping["_state_code"], "left").drop("_state_code")

    df = df.withColumn("country", F.lit("US"))
    df = df.withColumn("territory_code", F.col("state_code"))

    df = generate_surrogate_key(df, "location_id", key_name="location_key")

    df = df.select(
        "location_key",
        "location_id",
        "address_line1",
        "address_line2",
        "city",
        "county",
        "state_code",
        "state_name",
        "zip_code",
        "country",
        "latitude",
        "longitude",
        "territory_code",
        "flood_zone",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
