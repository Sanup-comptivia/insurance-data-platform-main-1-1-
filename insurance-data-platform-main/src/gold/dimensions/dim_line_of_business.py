"""
Gold Layer — dim_line_of_business dimension table.
Static reference dimension for the 5 insurance lines.
PK: lob_key (surrogate), NK: lob_code
"""

from pyspark.sql import SparkSession
from src.common.utils import generate_surrogate_key, write_delta_table

CATALOG = "insurance_catalog"
SCHEMA = "gold"
TABLE = "dim_line_of_business"


def build_dim_line_of_business(spark: SparkSession):
    """Build static LOB dimension."""
    print(f"  Building: {CATALOG}.{SCHEMA}.{TABLE}")

    data = [
        ("PROP", "Property", "Property", "State DOI", "04.0"),
        ("WC", "Workers Compensation", "Casualty", "NCCI / State WC Board", "16.0"),
        ("AUTO", "Auto", "Casualty", "State DOI", "19.4"),
        ("GL", "General Liability", "Casualty", "ISO / State DOI", "17.0"),
        ("UMB", "Umbrella", "Casualty", "ISO / State DOI", "17.3"),
    ]

    df = spark.createDataFrame(data, ["lob_code", "lob_name", "lob_category", "regulatory_body", "naic_code"])

    df = generate_surrogate_key(df, "lob_code", key_name="lob_key")

    df = df.select("lob_key", "lob_code", "lob_name", "lob_category", "regulatory_body", "naic_code")

    write_delta_table(df, CATALOG, SCHEMA, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
