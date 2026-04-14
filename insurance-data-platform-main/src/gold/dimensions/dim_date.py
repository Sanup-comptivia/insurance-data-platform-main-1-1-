"""
Gold Layer — dim_date dimension table.
Pre-generated date dimension covering 1970-01-01 to 2035-12-31.
PK: date_key (INT, YYYYMMDD format)

TRAINEE GUIDE — What is a Date Dimension?
Unlike other dimension tables that are built FROM source data, the date
dimension is PRE-GENERATED. It contains one row for every calendar day in a
fixed range (1970-01-01 to 2035-12-31) — roughly 24,000 rows. This means
every possible date already has a row BEFORE any policies or claims arrive.

Why pre-generate instead of deriving from data?
  - Consistency: All dates share the same attributes (day name, quarter, etc.)
    regardless of whether any business event occurred on that day.
  - Gap-free reporting: Even days with zero premium/zero claims have a row,
    enabling correct time-series analysis and calendar heat maps.

Key Concept — date_key (integer YYYYMMDD format):
  date_key is the PRIMARY KEY and is an integer in YYYYMMDD format.
  Example: January 15, 2024 → date_key = 20240115
  Why an integer instead of a DATE type?
    - Integers are faster to join on than DATE/TIMESTAMP types.
    - They are human-readable (you can eyeball "20240115" in a table).
    - This is a standard data-warehouse convention used across the industry.

Key Concept — Role-Playing Dimension:
  dim_date is a "role-playing dimension" — the SAME table is joined to fact
  tables via DIFFERENT foreign keys, each representing a different date role:
    fact_premium.date_key          → "when did coverage start?"
    dim_claim.date_of_loss_key     → "when did the loss occur?"
    dim_claim.date_reported_key    → "when was the claim reported?"
    dim_policy.effective_date_key  → "when does the policy begin?"
    dim_policy.expiration_date_key → "when does the policy end?"
  In SQL queries, you alias the same dim_date table multiple times:
    JOIN dim_date AS loss_date   ON claim.date_of_loss_key = loss_date.date_key
    JOIN dim_date AS report_date ON claim.date_reported_key = report_date.date_key

Key Concept — Fiscal Year vs Calendar Year:
  Calendar year: Jan 1 – Dec 31 (what most people think of as "a year").
  Fiscal year: An accounting year that may start on a different month.
  In this platform, fiscal year starts OCTOBER 1 (common in insurance and
  government). So October 2024 is fiscal_year=2025, fiscal_quarter=1.
  This matters for regulatory filings and annual financial statements.

Useful flags:
  is_weekend : True for Saturday/Sunday — used to exclude weekends in
               "business days" calculations (e.g. claim response SLA).
  is_holiday : Placeholder (set to False). In production, this would be
               enriched with a holiday calendar for business-day logic.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import write_delta_table

CATALOG = "insurance_catalog"
SCHEMA = "gold"
TABLE = "dim_date"


def build_dim_date(spark: SparkSession):
    """Generate a complete date dimension from 1970 to 2035."""
    print(f"  Building: {CATALOG}.{SCHEMA}.{TABLE}")

    # Generate date range
    start_date = "1970-01-01"
    end_date = "2035-12-31"

    df = (
        spark.sql(f"""
            SELECT explode(sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )) AS full_date
        """)
        .withColumn("date_key", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn("is_holiday", F.lit(False))  # Can be enriched with holiday calendar
        .withColumn(
            "fiscal_year", F.when(F.month("full_date") >= 10, F.year("full_date") + 1).otherwise(F.year("full_date"))
        )
        .withColumn(
            "fiscal_quarter",
            F.when(F.month("full_date").isin(10, 11, 12), 1)
            .when(F.month("full_date").isin(1, 2, 3), 2)
            .when(F.month("full_date").isin(4, 5, 6), 3)
            .otherwise(4),
        )
    )

    write_delta_table(df, CATALOG, SCHEMA, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df
