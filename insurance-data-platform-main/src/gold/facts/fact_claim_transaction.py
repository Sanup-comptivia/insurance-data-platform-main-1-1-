"""
Gold Layer — fact_claim_transaction fact table.
Grain: one row per claim payment/reserve transaction.
FK: claim_key, policy_key, date_key, lob_key

TRAINEE GUIDE — What does this Fact Table track?
This fact table records the MONEY FLOWING through claims — every payment,
reserve change, and recovery event. It answers questions like:
  - How much did we pay out on claim CLM-PROP-001?
  - What are our total reserves (money set aside) for open claims?
  - How much did we recover through subrogation?

How is this different from dim_claim?
  dim_claim stores the CLAIM ITSELF — who filed it, what happened, the status.
  fact_claim_transaction stores the FINANCIAL ACTIVITY — the money moving in
  and out over the life of that claim. One claim can have many transactions
  (initial reserve, partial payment, additional reserve, final payment, recovery).

The GRAIN of this table: ONE ROW per claim transaction (payment, reserve, or
recovery event). Example: Claim CLM-AUTO-042 might have three rows:
  Row 1: Initial reserve of $15,000 set on 2024-01-15
  Row 2: Partial payment of $8,000 made on 2024-02-01
  Row 3: Final payment of $6,500 made on 2024-03-10 (is_final_payment = True)

Key insurance financial concepts (claim-side):
  paid_amount     : Actual cash payments made to the claimant or provider.
                    This is real money leaving the insurer's bank account.
  reserve_amount  : Money SET ASIDE for expected future payments on this claim.
                    Reserves include IBNR (Incurred But Not Reported) — claims
                    that have happened but haven't been filed yet. Actuaries
                    estimate these. Reserves are a LIABILITY on the balance sheet.
  recovery_amount : Money RECOVERED by the insurer after paying a claim.
                    Two main sources:
                      Subrogation — claiming reimbursement from an at-fault
                                    third party (e.g., the other driver's insurer).
                      Salvage — selling damaged property (e.g., a totalled car).
  expense_amount  : Loss Adjustment Expenses (LAE) — the cost of HANDLING
                    the claim itself. Includes adjuster salaries, legal fees,
                    investigation costs, independent medical exams. These are
                    real costs above and beyond the claim payment.
  total_incurred  : paid + reserve + expense - recovery. This is the TOTAL
                    estimated cost of the claim to the insurer. It's the key
                    metric actuaries and underwriters watch.
  transaction_type: "Payment", "Reserve", or "Recovery" — classifies what
                    kind of money movement this row represents.
  is_final_payment: Boolean flag marking the LAST payment on a claim. When
                    True, it signals the claim is settling/closed. This is
                    important for reserving — once final payment is made,
                    remaining reserves should be released.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from src.common.utils import generate_surrogate_key, write_delta_table

CATALOG = "insurance_catalog"
SILVER = "silver"
GOLD = "gold"
TABLE = "fact_claim_transaction"


def build_fact_claim_transaction(spark: SparkSession):
    """Build claim transaction fact from Silver claims across all LOBs.

    TRAINEE NOTE — Why do we read from Silver (not Bronze) for facts?
    Silver data is typed and cleansed — numeric columns are actual decimals,
    dates are DateType, states are uppercase. Fact tables need these proper
    types for correct aggregation (you can't SUM string columns).

    TRAINEE NOTE — The "union all LOBs" pattern:
    Insurance companies typically store claims in separate tables per Line of
    Business (LOB): property claims, auto claims, workers' comp claims, etc.
    Each LOB has different source columns (e.g., "date_of_loss" for property
    vs. "date_of_injury" for WC vs. "date_of_accident" for auto).

    This method normalises all LOBs into a COMMON SCHEMA by:
      1. Reading each Silver LOB claim table separately
      2. Aliasing LOB-specific columns to standard names
         (e.g., date_of_loss → transaction_date, date_of_injury → transaction_date)
      3. Appending each normalised DataFrame to a list
      4. Unioning all frames into one combined DataFrame

    This pattern is used across all Gold fact tables in this platform.
    """
    print(f"  Building: {CATALOG}.{GOLD}.{TABLE}")

    # Load dimension lookup tables for FK resolution.
    # We only select the columns we need for joining — avoids loading full dims.
    claim_dim = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_claim", ["claim_key", "claim_id"])
    policy_dim = _safe_table(spark, f"{CATALOG}.{GOLD}.dim_policy", ["policy_key", "policy_id", "lob_key"])

    frames = []  # One DataFrame per LOB, unioned at the end

    # ---- Property Claims ----
    # TRAINEE NOTE: For each LOB, we select columns that map to the standard
    # claim transaction schema. LOB-specific column names are aliased to standard names:
    #   date_of_loss        → transaction_date (standard name)
    #   amount_paid_building → paid_amount (standard name)
    #   building_damage_amount → reserve_amount (estimate of total damage)
    # We also assign a hardcoded lob_code literal ("PROP") so each row can
    # later be matched to the correct policy in dim_policy.
    try:
        prop = spark.table(f"{CATALOG}.{SILVER}.silver_property_claims").select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("date_of_loss").alias("transaction_date"),
            F.lit("PROP").alias("lob_code"),
            F.lit("payment").alias("transaction_type"),
            F.coalesce(F.col("amount_paid_building"), F.lit(0)).cast(DecimalType(18, 2)).alias("paid_amount"),
            F.coalesce(F.col("building_damage_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("reserve_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("recovery_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("expense_amount"),
            F.when(F.col("claim_status") == "Closed", True).otherwise(False).alias("is_final_payment"),
        )
        frames.append(prop)
    except Exception:
        pass

    # ---- Workers' Comp Claims ----
    # WC claims combine medical_paid + indemnity_paid into paid_amount.
    # medical_paid = hospital/doctor bills; indemnity_paid = wage replacement.
    # total_incurred from the Silver table is used as reserve_amount here.
    try:
        wc = spark.table(f"{CATALOG}.{SILVER}.silver_workers_comp_claims").select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("date_of_injury").alias("transaction_date"),
            F.lit("WC").alias("lob_code"),
            F.lit("payment").alias("transaction_type"),
            (F.coalesce(F.col("medical_paid"), F.lit(0)) + F.coalesce(F.col("indemnity_paid"), F.lit(0)))
            .cast(DecimalType(18, 2))
            .alias("paid_amount"),
            F.coalesce(F.col("total_incurred"), F.lit(0)).cast(DecimalType(18, 2)).alias("reserve_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("recovery_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("expense_amount"),
            F.when(F.col("claim_status") == "Closed", True).otherwise(False).alias("is_final_payment"),
        )
        frames.append(wc)
    except Exception:
        pass

    # ---- Auto Claims ----
    # Auto uses total_claim_amount as paid_amount (full settlement) and
    # property_damage_amount as reserve_amount (damage estimate).
    try:
        auto = spark.table(f"{CATALOG}.{SILVER}.silver_auto_claims").select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("date_of_accident").alias("transaction_date"),
            F.lit("AUTO").alias("lob_code"),
            F.lit("payment").alias("transaction_type"),
            F.coalesce(F.col("total_claim_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("paid_amount"),
            F.coalesce(F.col("property_damage_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("reserve_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("recovery_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("expense_amount"),
            F.when(F.col("claim_status") == "Closed", True).otherwise(False).alias("is_final_payment"),
        )
        frames.append(auto)
    except Exception:
        pass

    # GL claims
    try:
        gl = spark.table(f"{CATALOG}.{SILVER}.silver_general_liability_claims").select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("date_of_occurrence").alias("transaction_date"),
            F.lit("GL").alias("lob_code"),
            F.lit("payment").alias("transaction_type"),
            F.coalesce(F.col("paid_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("paid_amount"),
            F.coalesce(F.col("reserve_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("reserve_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("recovery_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("expense_amount"),
            F.when(F.col("claim_status") == "Closed", True).otherwise(False).alias("is_final_payment"),
        )
        frames.append(gl)
    except Exception:
        pass

    # Umbrella claims
    try:
        umb = spark.table(f"{CATALOG}.{SILVER}.silver_umbrella_claims").select(
            F.col("claim_id"),
            F.col("policy_id"),
            F.col("date_of_occurrence").alias("transaction_date"),
            F.lit("UMB").alias("lob_code"),
            F.lit("payment").alias("transaction_type"),
            F.coalesce(F.col("excess_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("paid_amount"),
            F.coalesce(F.col("total_claim_amount"), F.lit(0)).cast(DecimalType(18, 2)).alias("reserve_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("recovery_amount"),
            F.lit(0).cast(DecimalType(18, 2)).alias("expense_amount"),
            F.when(F.col("claim_status") == "Closed", True).otherwise(False).alias("is_final_payment"),
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

    # Calculate total incurred
    df = df.withColumn(
        "total_incurred",
        (F.col("paid_amount") + F.col("reserve_amount") - F.col("recovery_amount")).cast(DecimalType(18, 2)),
    )

    # Date key
    df = df.withColumn(
        "date_key",
        F.when(F.col("transaction_date").isNotNull(), F.date_format(F.col("transaction_date"), "yyyyMMdd").cast("int")),
    )

    # Join claim key
    if claim_dim is not None:
        df = df.join(
            claim_dim.select(F.col("claim_key"), F.col("claim_id").alias("_cid")),
            df["claim_id"] == F.col("_cid"),
            "left",
        ).drop("_cid")
    else:
        df = df.withColumn("claim_key", F.lit(None).cast("string"))

    # Join policy key and lob_key
    if policy_dim is not None:
        df = df.join(
            policy_dim.select(
                F.col("policy_key"), F.col("policy_id").alias("_pol_id"), F.col("lob_key")
            ),
            df["policy_id"] == F.col("_pol_id"),
            "left",
        ).drop("_pol_id")
    else:
        df = df.withColumn("policy_key", F.lit(None).cast("string"))
        df = df.withColumn("lob_key", F.lit(None).cast("string"))

    df = generate_surrogate_key(df, "claim_id", "transaction_date", "transaction_type", key_name="claim_txn_key")

    df = df.select(
        "claim_txn_key",
        "claim_key",
        "policy_key",
        "date_key",
        "lob_key",
        "transaction_type",
        "paid_amount",
        "reserve_amount",
        "recovery_amount",
        "expense_amount",
        "total_incurred",
        "is_final_payment",
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
