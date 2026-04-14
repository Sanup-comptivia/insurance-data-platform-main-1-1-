"""
Gold Layer — dim_coverage dimension table.
Static + derived coverage types across all LOBs.
PK: coverage_key (surrogate), NK: coverage_id
"""

from pyspark.sql import SparkSession
from src.common.utils import generate_surrogate_key, write_delta_table

CATALOG = "insurance_catalog"
GOLD = "gold"
TABLE = "dim_coverage"


def build_dim_coverage(spark: SparkSession):
    """Build coverage dimension with standard insurance coverage types."""
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    # Get LOB keys for FK reference
    try:
        lob_df = spark.table(f"{CATALOG}.{GOLD}.dim_line_of_business").select("lob_key", "lob_code")
        lob_map = {row.lob_code: row.lob_key for row in lob_df.collect()}
    except Exception:
        lob_map = {"PROP": "prop", "WC": "wc", "AUTO": "auto", "GL": "gl", "UMB": "umb"}

    coverages = [
        # Property coverages
        (
            "COV-PROP-BLDG",
            "BLDG",
            "Building Coverage",
            "Property",
            "Covers the physical structure",
            lob_map.get("PROP", ""),
            True,
        ),
        (
            "COV-PROP-CONT",
            "CONT",
            "Contents Coverage",
            "Property",
            "Covers personal property/contents",
            lob_map.get("PROP", ""),
            False,
        ),
        (
            "COV-PROP-LOU",
            "LOU",
            "Loss of Use",
            "Property",
            "Covers additional living expenses",
            lob_map.get("PROP", ""),
            False,
        ),
        (
            "COV-PROP-ICC",
            "ICC",
            "Increased Cost of Compliance",
            "Property",
            "Covers floodplain compliance costs",
            lob_map.get("PROP", ""),
            False,
        ),
        # Workers' Comp coverages
        (
            "COV-WC-MED",
            "WC-MED",
            "Medical Benefits",
            "Workers Comp",
            "Medical treatment for work injuries",
            lob_map.get("WC", ""),
            True,
        ),
        (
            "COV-WC-IND",
            "WC-IND",
            "Indemnity Benefits",
            "Workers Comp",
            "Wage replacement benefits",
            lob_map.get("WC", ""),
            True,
        ),
        (
            "COV-WC-EL",
            "WC-EL",
            "Employers Liability",
            "Workers Comp",
            "Employers liability coverage",
            lob_map.get("WC", ""),
            True,
        ),
        # Auto coverages
        (
            "COV-AUTO-BI",
            "AUTO-BI",
            "Bodily Injury Liability",
            "Auto",
            "Covers injuries to others",
            lob_map.get("AUTO", ""),
            True,
        ),
        (
            "COV-AUTO-PD",
            "AUTO-PD",
            "Property Damage Liability",
            "Auto",
            "Covers damage to others property",
            lob_map.get("AUTO", ""),
            True,
        ),
        (
            "COV-AUTO-COMP",
            "AUTO-COMP",
            "Comprehensive",
            "Auto",
            "Non-collision damage coverage",
            lob_map.get("AUTO", ""),
            False,
        ),
        (
            "COV-AUTO-COLL",
            "AUTO-COLL",
            "Collision",
            "Auto",
            "Collision damage coverage",
            lob_map.get("AUTO", ""),
            False,
        ),
        (
            "COV-AUTO-UM",
            "AUTO-UM",
            "Uninsured Motorist",
            "Auto",
            "Protection from uninsured drivers",
            lob_map.get("AUTO", ""),
            False,
        ),
        # General Liability coverages
        (
            "COV-GL-PREM",
            "GL-PREM",
            "Premises Liability",
            "General Liability",
            "Covers on-premises injuries",
            lob_map.get("GL", ""),
            True,
        ),
        (
            "COV-GL-PROD",
            "GL-PROD",
            "Products Liability",
            "General Liability",
            "Covers product-related injuries",
            lob_map.get("GL", ""),
            False,
        ),
        (
            "COV-GL-COMP",
            "GL-COMP",
            "Completed Operations",
            "General Liability",
            "Covers post-completion claims",
            lob_map.get("GL", ""),
            False,
        ),
        (
            "COV-GL-PI",
            "GL-PI",
            "Personal Injury",
            "General Liability",
            "Covers non-physical injuries",
            lob_map.get("GL", ""),
            False,
        ),
        # Umbrella coverages
        (
            "COV-UMB-EXCESS",
            "UMB-EXCESS",
            "Excess Liability",
            "Umbrella",
            "Excess coverage above underlying",
            lob_map.get("UMB", ""),
            True,
        ),
        (
            "COV-UMB-DROP",
            "UMB-DROP",
            "Drop Down Coverage",
            "Umbrella",
            "Coverage when underlying exhausted",
            lob_map.get("UMB", ""),
            False,
        ),
    ]

    df = spark.createDataFrame(
        coverages,
        [
            "coverage_id",
            "coverage_code",
            "coverage_name",
            "coverage_type",
            "coverage_description",
            "lob_key",
            "is_mandatory",
        ],
    )

    df = generate_surrogate_key(df, "coverage_id", key_name="coverage_key")

    df = df.select(
        "coverage_key",
        "coverage_id",
        "coverage_code",
        "coverage_name",
        "coverage_type",
        "coverage_description",
        "lob_key",
        "is_mandatory",
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
