"""
Gold Layer — dim_policy dimension table.
Central policy dimension joining all LOB policies.
PK: policy_key (surrogate), NK: policy_id
FK: lob_key, insured_key, agent_key, location_key, effective_date_key, expiration_date_key

TRAINEE GUIDE — What is a Dimension Table?
In a Star Schema (the analytics pattern used in the Gold layer), data is
organised into two types of tables:
  - Dimension tables: The "WHO/WHAT/WHERE/WHEN" context of a business event.
                      They describe entities like policies, customers, agents.
  - Fact tables: The "HOW MUCH/HOW MANY" measurements (premiums, claims).

dim_policy is the CENTRAL dimension — it is the hub from which all
premium and claim facts can be traced back to a specific policy, LOB,
insured person, and agent.

Key Concept — Surrogate Keys vs Natural Keys:
  Natural key (NK): The business identifier — "PROP-0000000001" (policy_id).
                    Comes from the source system. May change, be reused, or
                    contain special characters.
  Surrogate key (PK): A stable, system-generated identifier (MD5 hash).
                      Never changes. Used for joins between fact and dim tables.

Key Concept — Foreign Keys (FK):
  dim_policy holds surrogate keys pointing to other dimensions:
    lob_key      → dim_line_of_business (which line of business?)
    insured_key  → dim_insured (who is insured?)
    agent_key    → dim_agent (which agent sold it?)
    location_key → dim_location (where is the insured property/vehicle?)
    effective_date_key  → dim_date (when does coverage start?)
    expiration_date_key → dim_date (when does coverage end?)

  This FK pattern is the "star" in star schema — dim_policy is the center,
  and the other dimensions radiate outward from it.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import generate_surrogate_key, deduplicate, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "dim_policy"


def build_dim_policy(spark: SparkSession):
    """Build policy dimension from all Silver LOB policy tables.

    TRAINEE NOTE — The Union-All-LOBs Pattern
    This function is the key pattern in the Gold layer: it reads from ALL
    5 Silver LOB tables and UNIONS them into a single dimension.

    Why do we have 5 separate Silver tables but ONE Gold dimension?
      Silver keeps LOBs separate because each LOB has unique columns
      (flood_zone for Property, vehicle_year for Auto, etc.).
      Gold normalises them to a common set of columns that apply to ALL
      policies regardless of LOB. LOB-specific attributes appear only in
      the mart (mart_policy_360) where we join Silver LOB tables back in.

    Graceful degradation (try/except per LOB):
      Each LOB block is wrapped in try/except. If a Silver table doesn't
      exist (e.g. the pipeline hasn't run for that LOB yet), we skip it
      and continue with the others. This makes the function robust during
      partial pipeline runs and development.

    Build order dependency:
      dim_policy depends on dim_line_of_business, dim_insured, and dim_agent
      being built FIRST (it joins them to get FK keys). The orchestration in
      notebooks/run_all_pipeline.py ensures this order.
    """
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    # Load reference dimensions for FK key lookups.
    # If a dimension table doesn't exist yet, _safe_table returns None.
    # The join sections below handle None gracefully by setting the key to null.
    lob_df = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_line_of_business")
    insured_df = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_insured")
    agent_df = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_agent")
    _safe_table(spark, f"{CATALOG}.{GOLD}.dim_location")  # Loaded but not used yet (see location_key note below)

    frames = []  # Will hold one DataFrame per LOB; unioned at the end

    # ---- Property ----
    # TRAINEE NOTE: Each LOB block does the same thing:
    #   1. Read the LOB's Silver table
    #   2. Select columns that make sense for ALL policies (not LOB-specific)
    #   3. Add a literal lob_code column ("PROP", "WC", "AUTO", "GL", "UMB")
    #   4. Append to the frames list
    # The lob_code is used later to join to dim_line_of_business.
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_policies").select(
            F.col("policy_id"),
            F.col("policy_number"),
            F.lit("PROP").alias("lob_code"),          # Hard-code the LOB code for this table
            F.col("policy_id").alias("insured_id"),   # In property, the insured ID = policy_id
            F.col("agent_id"),
            F.col("effective_date"),
            F.col("expiration_date"),
            F.lit("Active").alias("policy_status"),   # Default status (source doesn't have this)
            F.col("underwriter"),
        )
        frames.append(prop)
    except Exception:
        pass  # Property Silver not found — skip and try remaining LOBs

    # ---- Workers' Comp ----
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_policies").select(
            F.col("policy_id"),
            F.col("policy_number"),
            F.lit("WC").alias("lob_code"),
            F.col("policy_id").alias("insured_id"),
            F.col("agent_id"),
            F.col("effective_date"),
            F.col("expiration_date"),
            F.lit("Active").alias("policy_status"),
            F.col("underwriter"),
        )
        frames.append(wc)
    except Exception:
        pass

    # ---- Auto ----
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_policies").select(
            F.col("policy_id"),
            F.col("policy_number"),
            F.lit("AUTO").alias("lob_code"),
            F.col("policy_id").alias("insured_id"),
            F.col("agent_id"),
            F.col("effective_date"),
            F.col("expiration_date"),
            F.lit("Active").alias("policy_status"),
            F.col("underwriter"),
        )
        frames.append(auto)
    except Exception:
        pass

    # ---- General Liability ----
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_policies").select(
            F.col("policy_id"),
            F.col("policy_number"),
            F.lit("GL").alias("lob_code"),
            F.col("policy_id").alias("insured_id"),
            F.col("agent_id"),
            F.col("effective_date"),
            F.col("expiration_date"),
            F.lit("Active").alias("policy_status"),
            F.col("underwriter"),
        )
        frames.append(gl)
    except Exception:
        pass

    # ---- Umbrella ----
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_policies").select(
            F.col("policy_id"),
            F.col("policy_number"),
            F.lit("UMB").alias("lob_code"),
            F.col("policy_id").alias("insured_id"),
            F.col("agent_id"),
            F.col("effective_date"),
            F.col("expiration_date"),
            F.lit("Active").alias("policy_status"),
            F.col("underwriter"),
        )
        frames.append(umb)
    except Exception:
        pass

    if not frames:
        print("    WARN: No Silver data found. Skipping.")
        return None

    # Union all LOB DataFrames into one.
    # unionByName: match columns by name, not position (safe when schemas differ slightly)
    df = frames[0]
    for f in frames[1:]:
        df = df.unionByName(f)

    # Deduplicate by policy_id.
    # TRAINEE NOTE: We order by effective_date descending so that if the same
    # policy_id appears in multiple LOBs (unlikely but possible), we keep the
    # most recently effective one. In practice, policy_id includes the LOB
    # prefix (e.g. "PROP-0001") so cross-LOB duplicates should not occur.
    df = deduplicate(df, key_columns=["policy_id"], order_column="effective_date", ascending=False)

    # ---- Join LOB key (FK to dim_line_of_business) ----
    # TRAINEE NOTE — Left join pattern for FK lookups:
    #   We JOIN dim_line_of_business ON lob_code to get the lob_key surrogate.
    #   We use a LEFT JOIN (not INNER JOIN) so that:
    #     - Policies WITH a matching LOB code get their lob_key populated.
    #     - Policies WITHOUT a match (unusual, data error) still appear in
    #       the output with lob_key=null, rather than being silently dropped.
    #   Alias trick: We rename lob_df["lob_code"] to "_lob_code" before joining
    #   to avoid column name ambiguity (both DataFrames have a "lob_code" column).
    if lob_df is not None:
        df = df.join(
            lob_df.select(F.col("lob_key"), F.col("lob_code").alias("_lob_code")),
            df["lob_code"] == F.col("_lob_code"),
            "left",
        ).drop("_lob_code")  # Remove the alias column after the join
    else:
        # dim_line_of_business not built yet — set FK to null for now
        df = df.withColumn("lob_key", F.lit(None).cast("string"))

    # ---- Join insured key (FK to dim_insured) ----
    if insured_df is not None:
        df = df.join(
            insured_df.select(F.col("insured_key"), F.col("insured_id").alias("_insured_id")),
            df["insured_id"] == F.col("_insured_id"),
            "left",
        ).drop("_insured_id")
    else:
        df = df.withColumn("insured_key", F.lit(None).cast("string"))

    # ---- Join agent key (FK to dim_agent) ----
    if agent_df is not None:
        df = df.join(
            agent_df.select(F.col("agent_key"), F.col("agent_id").alias("_agent_id")),
            df["agent_id"] == F.col("_agent_id"),
            "left",
        ).drop("_agent_id")
    else:
        df = df.withColumn("agent_key", F.lit(None).cast("string"))

    # ---- Location key placeholder ----
    # TRAINEE NOTE: In a production system, location would be derived by
    # geocoding the address (street → lat/long → location_key). This requires
    # a geocoding API or a reference dataset. For now, we set it to null as
    # a placeholder that can be filled in when the geocoding step is added.
    df = df.withColumn("location_key", F.lit(None).cast("string"))

    # ---- Date keys (FK to dim_date) ----
    # TRAINEE NOTE: Date keys are integer YYYYMMDD values that link to dim_date.
    # F.when(condition, value): only compute the date format if the date is not null.
    # F.date_format("2024-01-15", "yyyyMMdd") → "20240115" → cast to int → 20240115
    df = df.withColumn(
        "effective_date_key",
        F.when(F.col("effective_date").isNotNull(), F.date_format(F.col("effective_date"), "yyyyMMdd").cast("int")),
    ).withColumn(
        "expiration_date_key",
        F.when(F.col("expiration_date").isNotNull(), F.date_format(F.col("expiration_date"), "yyyyMMdd").cast("int")),
    )

    # ---- Derive additional business fields ----
    # TRAINEE NOTE:
    #   policy_term_months: months_between(end, start) gives the duration
    #     of the policy. Most policies are 12 months (annual) but some are 6.
    #
    #   risk_tier: A simplified mock risk classification using a hash of
    #     policy_id. In production this would come from an underwriting model
    #     or pricing engine. F.abs(F.hash("policy_id")) % 3 maps each policy
    #     to 0, 1, or 2 — then +1 makes it 1/2/3 for F.element_at indexing.
    #     element_at(array, index) is 1-based in Spark (index starts at 1).
    #
    #   is_renewal / original_policy_id: Flags for renewal policies (not yet
    #     populated — placeholder for a future renewal detection step).
    df = (
        df.withColumn(
            "policy_term_months", F.months_between(F.col("expiration_date"), F.col("effective_date")).cast("int")
        )
        .withColumn(
            "risk_tier",
            F.element_at(
                F.array(F.lit("Preferred"), F.lit("Standard"), F.lit("Substandard")),
                (F.abs(F.hash("policy_id")) % 3) + 1,  # Deterministic pseudo-random tier
            ),
        )
        .withColumn("is_renewal", F.lit(False))               # Renewal detection not yet implemented
        .withColumn("original_policy_id", F.lit(None).cast("string"))  # FK for renewals (future)
    )

    # ---- Generate surrogate key ----
    # The surrogate key is generated from policy_id only (natural key).
    # We use only policy_id (not lob_code) because policy_id already contains
    # the LOB prefix (e.g. "PROP-001"), making it globally unique.
    df = generate_surrogate_key(df, "policy_id", key_name="policy_key")

    # ---- Final column selection ----
    # Explicitly select and order the output columns for a clean, consistent schema.
    df = df.select(
        "policy_key",           # Surrogate PK (MD5 hash)
        "policy_id",            # Natural key (from source system)
        "policy_number",        # Human-readable policy number
        "lob_key",              # FK → dim_line_of_business
        "insured_key",          # FK → dim_insured
        "agent_key",            # FK → dim_agent
        "location_key",         # FK → dim_location (placeholder)
        "effective_date_key",   # FK → dim_date (start of coverage)
        "expiration_date_key",  # FK → dim_date (end of coverage)
        "policy_status",        # Active / Cancelled / Expired
        "policy_term_months",   # Duration in months (typically 12)
        "underwriter",          # Person who underwrote the policy
        "risk_tier",            # Preferred / Standard / Substandard
        "is_renewal",           # True if this is a renewal of a prior policy
        "original_policy_id",   # For renewals: the prior policy's ID
    )

    write_delta_table(df, CATALOG, GOLD, TABLE, mode="overwrite")
    print(f"    Rows: {df.count():,}")
    return df


def _safe_table(spark, table_name):
    """Load a table if it exists, return None if it doesn't.

    TRAINEE NOTE:
    This helper is used throughout the Gold layer when loading reference
    dimensions. If a dimension hasn't been built yet (e.g. we're rebuilding
    just one table), we return None instead of crashing. The calling code
    then falls back to setting the FK column to null.

    This makes the Gold build functions "re-entrant" — you can run them
    independently without requiring all other dimensions to exist first.
    """
    try:
        return spark.table(table_name)
    except Exception:
        return None
