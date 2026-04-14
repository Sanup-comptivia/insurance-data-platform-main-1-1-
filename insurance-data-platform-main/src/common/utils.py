"""
Shared utility functions for the Insurance Data Platform.
Provides surrogate key generation, metadata columns, and common helpers.

TRAINEE GUIDE — This file is the "toolbox" for the entire pipeline.
Every layer (Bronze, Silver, Gold) imports from here. Understanding these
functions will help you understand how data moves and transforms throughout
the platform.

Key concepts covered in this file:
  - SparkSession: The entry point to all Spark operations
  - Surrogate keys: Stable IDs we generate ourselves (instead of database sequences)
  - Metadata columns: Audit trail columns added to every Bronze table
  - Data Quality (DQ) flags: How we mark invalid rows instead of dropping them
  - Delta Lake writes: How we persist DataFrames as queryable tables
  - Snake_case: Naming convention required for SQL compatibility
  - Deduplication: Keeping only the latest version of each record
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import uuid


def get_spark() -> SparkSession:
    """Get or create SparkSession with Delta Lake support.

    TRAINEE NOTE:
    SparkSession is the main entry point for PySpark.
    Think of it as the "connection" to the Spark cluster.

    Config breakdown:
      - spark.sql.extensions: Loads the Delta Lake SQL extension so we can
        use Delta-specific SQL commands (e.g. MERGE INTO, VACUUM).
      - spark.sql.catalog.spark_catalog: Replaces the default catalog with
        Delta's catalog so all table reads/writes use Delta format by default.
      - spark.sql.adaptive.enabled: Turns on Adaptive Query Execution (AQE),
        which lets Spark automatically tune query plans at runtime — helps
        performance without manual tuning.
      - spark.sql.shuffle.partitions: Controls how many partitions Spark
        creates after a shuffle (join, groupBy). 200 is a safe default for
        medium datasets; tune higher for very large data.

    On Databricks, a SparkSession is already created for you. This function
    is mainly used for local unit testing outside Databricks.
    """
    return (
        SparkSession.builder.appName("InsuranceDataPlatform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def generate_surrogate_key(df: DataFrame, *key_columns: str, key_name: str = "surrogate_key") -> DataFrame:
    """
    Generate deterministic MD5-based surrogate key from natural key columns.
    Enables idempotent loads without needing sequence generators.

    Args:
        df: Input DataFrame
        key_columns: Column names to hash into the surrogate key
        key_name: Name for the generated key column
    Returns:
        DataFrame with added surrogate key column

    TRAINEE NOTE — What is a surrogate key?
    A surrogate key is an artificial, system-generated unique identifier
    for a record. In traditional databases this is usually an AUTO_INCREMENT
    integer. In distributed Spark pipelines we cannot use sequences because
    there is no single coordinator, so we use a hash instead.

    Why MD5 (deterministic hash)?
    - Deterministic: Given the same input, you always get the same output.
      This means you can re-run the pipeline and the same policy will always
      get the same key — no duplicates, no orphan FK references.
    - Compact: MD5 produces a fixed 32-character hex string regardless of
      how long the input is.
    - Fast: Spark can compute MD5 in parallel across the cluster.

    How it works step by step:
      1. coalesce(col, "__NULL__"): If a key column is null, replace it with
         the literal string "__NULL__" so the hash is still computed (null
         hashing would return null, breaking the key).
      2. concat_ws("||", ...): Concatenate all key columns separated by "||"
         to form a single string. The separator "||" is chosen because it is
         unlikely to appear inside actual data values.
      3. F.md5(...): Hash the concatenated string into a 32-char hex key.

    Example:
      policy_id="PROP-001", lob_code="PROP"
      → concat: "PROP-001||PROP"
      → md5:    "a3f4b2c1..."  (always the same for this input)
    """
    concat_expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast(StringType()), F.lit("__NULL__")) for c in key_columns])
    return df.withColumn(key_name, F.md5(concat_expr))


def add_metadata_columns(
    df: DataFrame,
    source_file: str = "unknown",
    source_format: str = "unknown",
    batch_id: str = None,
) -> DataFrame:
    """
    Add standard metadata columns to a DataFrame for Bronze layer tracking.

    Args:
        df: Input DataFrame
        source_file: Name/path of the source file
        source_format: Format of source (csv, json, xml, parquet)
        batch_id: Unique batch identifier (auto-generated if None)
    Returns:
        DataFrame with metadata columns added

    TRAINEE NOTE — Why do we add metadata columns?
    These columns form the "audit trail" — a record of where each row came
    from and when it was loaded. This is critical in a data platform because:

      1. Debugging: If a row looks wrong, you can trace it back to the exact
         source file and load timestamp.
      2. Reprocessing: If we need to replay a load, the batch_id tells us
         which records belong to which run.
      3. Deduplication: The _ingestion_timestamp is used by the deduplicate()
         function below to keep the most recent version of a record.
      4. Compliance / Lineage: Auditors and data governance teams need to know
         data provenance (where it came from).

    The four metadata columns added:
      _ingestion_timestamp : When this row was loaded (server time, UTC)
      _source_file         : The file path or API endpoint that was read
      _source_format       : csv / json / xml / parquet
      _batch_id            : A UUID that groups all rows loaded in one run.
                             If batch_id is not provided, a new UUID is auto-
                             generated for this call. This means all rows in
                             a single ingest_source() call share the same ID.

    These columns all start with "_" by convention to signal that they are
    system/technical columns, not business data.
    """
    if batch_id is None:
        # uuid4() generates a random universally unique identifier.
        # Each pipeline run gets a different batch_id, which lets us
        # trace or roll back a specific load if needed.
        batch_id = str(uuid.uuid4())

    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_format", F.lit(source_format))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def add_dq_columns(df: DataFrame, dq_checks: list = None) -> DataFrame:
    """
    Add data quality flag columns based on validation checks.

    Args:
        df: Input DataFrame
        dq_checks: List of tuples (check_name, condition_expr) where condition_expr
                   is a Column expression that returns True when the check FAILS.
    Returns:
        DataFrame with _is_valid (bool) and _dq_issues (string) columns

    TRAINEE NOTE — The "flag, don't drop" strategy
    A common beginner mistake is to filter out (drop) bad rows immediately.
    We intentionally do NOT drop bad rows. Instead we FLAG them with:
      _is_valid  : True if all checks passed, False if any check failed
      _dq_issues : A semicolon-separated list of which checks failed

    Why keep bad rows?
      - Downstream teams may still need the partial data (e.g., a claim with
        a missing date is still a claim — the amount is still valid).
      - It lets data engineers and analysts investigate root causes.
      - It prevents silent data loss — you can always see how many records
        were flagged and why.

    Where do bad rows go?
      - Silver tables keep ALL rows (good + bad).
      - Gold dimensions and facts typically filter to only _is_valid=True rows
        so that analytics are not polluted.

    How the check logic works:
      Each check in dq_checks is a tuple: (name, condition_expr)
      The condition_expr is a Spark Column expression that is TRUE when the
      check FAILS (i.e., the row is BAD). For example:
        ("null_policy_id", F.col("policy_id").isNull())
        → True when policy_id IS null  → check fails → flagged

      We collect all failed check names into an array, then:
        _is_valid  = (array length == 0)   — no failures → valid
        _dq_issues = array joined by "; "  — e.g. "null_policy_id; non_positive_premium"
    """
    if dq_checks is None or len(dq_checks) == 0:
        # No checks requested — mark everything as valid with no issues.
        return df.withColumn("_is_valid", F.lit(True)).withColumn("_dq_issues", F.lit(None).cast(StringType()))

    # Build array of failed check names.
    # F.when(condition, value): returns value when condition is True, else null.
    # So each element is either the check name (if failed) or null (if passed).
    issue_conditions = []
    for check_name, condition_expr in dq_checks:
        issue_conditions.append(F.when(condition_expr, F.lit(check_name)))

    # F.array_compact removes nulls from the array, leaving only failed check names.
    issues_array = F.array_compact(F.array(*issue_conditions))

    return (
        df.withColumn("_dq_issues_arr", issues_array)
        # A row is valid only if zero checks failed (array is empty).
        .withColumn("_is_valid", F.size(F.col("_dq_issues_arr")) == 0)
        # Join failed check names into a single readable string.
        .withColumn("_dq_issues", F.array_join(F.col("_dq_issues_arr"), "; "))
        .drop("_dq_issues_arr")  # Clean up the intermediate helper column.
    )


def write_delta_table(
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    mode: str = "overwrite",
    partition_by: list = None,
    merge_keys: list = None,
):
    """
    Write DataFrame as a Delta table to Unity Catalog.

    Args:
        df: DataFrame to write
        catalog: Unity Catalog name
        schema: Schema/database name
        table: Table name
        mode: Write mode (overwrite, append, merge)
        partition_by: List of columns to partition by
        merge_keys: For merge mode, list of key columns for matching

    TRAINEE NOTE — Unity Catalog naming convention
    Databricks Unity Catalog uses a three-level namespace:
      catalog.schema.table
      e.g.  insurance_catalog.bronze.bronze_property_policies_csv

    This is analogous to a traditional database's:
      database.schema.table

    Write modes explained:
      overwrite : Drop and recreate the table on every run.
                  Used in Bronze and Silver where we reload from source.
                  This makes the pipeline IDEMPOTENT (safe to re-run).

      append    : Add rows to an existing table (does not delete old rows).
                  Used when data is truly incremental and old rows must be kept.

      merge     : Delta Lake MERGE INTO — upsert pattern.
                  Update existing rows if the key matches; insert new rows.
                  Used when we want to maintain a slowly changing table
                  without full reload (more efficient for large tables).

    partition_by:
      Partitioning physically organises files on disk by column value (e.g.
      by state_code or year). Queries that filter on the partition column skip
      irrelevant files entirely — called "partition pruning". Only partition
      on high-cardinality columns that are frequently filtered.
    """
    full_table_name = f"{catalog}.{schema}.{table}"

    if mode == "merge" and merge_keys:
        _merge_into_delta(df, full_table_name, merge_keys)
    else:
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(full_table_name)


def _merge_into_delta(df: DataFrame, target_table: str, merge_keys: list):
    """Perform MERGE INTO for upsert operations using Delta Lake.

    TRAINEE NOTE — What is MERGE INTO (upsert)?
    MERGE INTO is a SQL/Delta Lake operation that combines INSERT and UPDATE
    into a single atomic transaction:
      - If a row with matching merge_keys already exists in the target table
        → UPDATE all columns to the new values (whenMatchedUpdateAll)
      - If no matching row exists
        → INSERT the new row (whenNotMatchedInsertAll)

    This is the "slowly changing dimension" (SCD Type 1) pattern — we simply
    overwrite old values with new ones. For SCD Type 2 (keep history), you
    would add an "effective_to" date instead.

    The DeltaTable.isDeltaTable() check ensures that if the target table does
    not exist yet, we do a simple write instead of a MERGE (which would fail
    on a non-existent table).
    """
    from delta.tables import DeltaTable

    spark = df.sparkSession

    if not DeltaTable.isDeltaTable(spark, target_table):
        # First load — table doesn't exist yet, so just write it.
        df.write.format("delta").saveAsTable(target_table)
        return

    dt = DeltaTable.forName(spark, target_table)
    # Build the join condition: "target.policy_id = source.policy_id AND ..."
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    (
        dt.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()    # Row exists → overwrite all columns
        .whenNotMatchedInsertAll() # Row is new → insert it
        .execute()
    )


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Convert all column names to snake_case.

    TRAINEE NOTE — Why do we standardise column names?
    Different data sources use different naming conventions:
      CSV from FEMA     : "totalBuildingCoverage" (camelCase)
      JSON from API     : "TotalBuildingCoverage" (PascalCase)
      Our Silver tables : "total_building_coverage" (snake_case)

    Problems with inconsistent naming:
      1. SQL is case-insensitive but Spark/Python is case-sensitive for columns.
      2. Mixed conventions make joins and unions fail silently if column names
         don't match exactly.
      3. snake_case is required for PySpark's .withColumn() and SQL SELECT.

    How the regex works:
      Step 1: re.sub("(.)([A-Z][a-z]+)", r"\\1_\\2", name)
              Inserts "_" before a capital letter followed by lowercase letters.
              "totalBuildingCoverage" → "total_Building_Coverage"

      Step 2: re.sub("([a-z0-9])([A-Z])", r"\\1_\\2", s1).lower()
              Inserts "_" before a capital that follows a lowercase/digit.
              → "total_building_coverage"

      Step 3: Replace non-alphanumeric characters (spaces, dots, dashes) with "_".
      Step 4: Collapse multiple consecutive "_" into one, strip leading/trailing "_".
    """
    import re

    new_columns = []
    for col_name in df.columns:
        # Convert camelCase to snake_case
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", col_name)
        snake = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
        # Replace spaces and special chars with underscore
        snake = re.sub(r"[^a-z0-9_]", "_", snake)
        # Remove consecutive underscores
        snake = re.sub(r"_+", "_", snake).strip("_")
        new_columns.append(snake)

    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    return df


def deduplicate(
    df: DataFrame, key_columns: list, order_column: str = "_ingestion_timestamp", ascending: bool = False
) -> DataFrame:
    """
    Deduplicate DataFrame by keeping the latest record per key.

    Args:
        df: Input DataFrame
        key_columns: Columns that define uniqueness
        order_column: Column to order by for choosing which record to keep
        ascending: If True, keep first; if False, keep latest

    TRAINEE NOTE — Why do we deduplicate?
    Data arrives from multiple sources (CSV + JSON for the same policies).
    If we union both sources naively, the same policy appears twice. We only
    want ONE canonical version per policy.

    How the Window function works:
      A Window partitions the data by key_columns (like GROUP BY) but instead
      of aggregating, it assigns a row number within each partition.

      Window.partitionBy("policy_id")   → group rows by policy_id
      .orderBy(order_column.desc())     → within each group, sort newest first

      row_number() = 1 is the newest record per policy_id.
      We then filter to only row_number = 1 and drop the helper column.

    Visual example:
      policy_id | _ingestion_timestamp | row_num
      PROP-001  | 2024-01-02 10:00:00  |    1   ← KEEP (newest)
      PROP-001  | 2024-01-01 08:00:00  |    2   ← DROP (older)
      PROP-002  | 2024-01-03 14:00:00  |    1   ← KEEP

    Why use row_number() instead of dropDuplicates()?
      dropDuplicates() picks an arbitrary row — we want to pick the LATEST.
      row_number() + orderBy gives us explicit control over which row to keep.
    """
    from pyspark.sql.window import Window

    # If the specified order column doesn't exist, add a monotonically increasing id as fallback
    if order_column not in df.columns:
        df = df.withColumn(order_column, F.monotonically_increasing_id())

    order_expr = F.col(order_column).asc() if ascending else F.col(order_column).desc()
    window = Window.partitionBy(*key_columns).orderBy(order_expr)

    return df.withColumn("_row_num", F.row_number().over(window)).filter(F.col("_row_num") == 1).drop("_row_num")


def date_to_key(date_col: str) -> F.Column:
    """Convert a date column to integer date key (YYYYMMDD format).

    TRAINEE NOTE — Why use an integer date key instead of a date type?
    In star schema design, dimension tables often use integer surrogate keys
    for performance. For dates, the YYYYMMDD integer format (e.g. 20240115)
    is preferred because:
      - It sorts correctly as an integer (no need to parse).
      - It is human-readable (you can see the date at a glance).
      - Joins between fact and dim_date tables are fast on integer columns.
      - It matches the standard used in most data warehouses (Kimball design).

    Example: date 2024-01-15 → integer 20240115
    """
    return F.date_format(F.col(date_col), "yyyyMMdd").cast("int")


# US state code to name mapping
# TRAINEE NOTE: This lookup dictionary is used in the Gold layer to enrich
# location data with the full state name (e.g. "CA" → "California").
# It is defined here so any module can import it without re-defining it.
US_STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}
