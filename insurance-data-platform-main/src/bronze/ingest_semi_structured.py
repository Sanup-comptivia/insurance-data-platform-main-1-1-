"""
Bronze Layer — Semi-Structured Data Ingestion (JSON, XML → Delta).
Preserves raw nested/complex structures in Delta tables.

TRAINEE GUIDE — Structured vs. Semi-Structured data
Structured data (CSV, Parquet) has a rigid, flat, tabular format: every row
has the same columns in the same order. Semi-structured data (JSON, XML) is
more flexible — records can have nested objects, arrays, and optional fields.

Examples in this platform:
  Structured  : FEMA bulk CSV downloads (fixed columns, millions of rows)
  Semi-struct : FEMA API responses (JSON with metadata wrapper + nested records)
                Insurance XML feeds from partner systems

Why separate classes?
  The reading logic is meaningfully different:
    - CSV: spark.read.csv() with simple options
    - JSON: handle nested structures, corrupt records, multiline
    - XML: requires an external library (spark-xml)
  Keeping them in separate classes makes each easier to test and extend.

Corrupt record handling:
  JSON APIs sometimes return malformed records. We use mode="PERMISSIVE" so
  Spark continues reading even if some records are malformed, placing the raw
  malformed JSON into a special "_corrupt_record" column. This way, bad records
  are visible for investigation rather than silently lost.

Usage (Databricks):
    from src.bronze.ingest_semi_structured import BronzeSemiStructuredIngestion
    ingestion = BronzeSemiStructuredIngestion(spark)
    ingestion.ingest_all()
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.common.utils import add_metadata_columns, write_delta_table

CATALOG = "insurance_catalog"
SCHEMA = "bronze"


class BronzeSemiStructuredIngestion:
    """Ingest semi-structured (JSON/XML) files into Bronze Delta tables."""

    def __init__(self, spark: SparkSession, raw_base_path: str = "/Volumes/insurance_catalog/bronze/raw_data"):
        self.spark = spark
        self.raw_base = raw_base_path

    def ingest_all(self):
        """Ingest all semi-structured data sources into Bronze layer.

        TRAINEE NOTE:
        Same ingestion_map pattern as the structured ingester. The try/except
        per source ensures one bad file does not abort the whole run.

        Note how each LOB (line of business) may supply data in DIFFERENT
        formats depending on the source system:
          - Workers' Comp claims: JSON (from a REST API)
          - Auto policies: JSON (synthetic generated)
          - General Liability claims: JSON (synthetic generated)

        This is realistic: in enterprise environments, different source
        systems use different formats and protocols.
        """
        print("=" * 60)
        print("Bronze Layer — Semi-Structured Data Ingestion")
        print("=" * 60)

        # Each tuple: (relative path under raw_base, target Delta table name, format)
        ingestion_map = [
            # Property: both CSV and JSON versions exist (from different sources)
            ("property/synthetic_policies_json", "bronze_property_policies_json", "json"),
            ("property/synthetic_claims_json", "bronze_property_claims_json", "json"),
            ("property/fema_nfip_claims", "bronze_property_fema_claims_json", "json"),   # FEMA API response
            ("property/fema_nfip_policies", "bronze_property_fema_policies_json", "json"),
            # Workers' Comp claims come in as JSON (policies are CSV)
            ("workers_comp/synthetic_claims_json", "bronze_workers_comp_claims_json", "json"),
            # Auto policies in JSON form (in addition to CSV above)
            ("auto/synthetic_policies_json", "bronze_auto_policies_json", "json"),
            # GL claims only in JSON
            ("general_liability/synthetic_claims_json", "bronze_general_liability_claims_json", "json"),
            # Umbrella in JSON
            ("umbrella/synthetic_policies_json", "bronze_umbrella_policies_json", "json"),
        ]

        for source_suffix, table_name, fmt in ingestion_map:
            source_path = f"{self.raw_base}/{source_suffix}"
            try:
                self._ingest_source(source_path, table_name, fmt)
            except Exception as e:
                print(f"  WARN: Skipping {table_name} — {str(e)}")

        print("\nBronze semi-structured ingestion complete.")

    def _ingest_source(self, source_path: str, table_name: str, fmt: str):
        """Ingest a single semi-structured source into a Bronze Delta table."""
        print(f"\n  Ingesting: {table_name}")
        print(f"    Source: {source_path}")

        if fmt == "json":
            df = self._read_json(source_path)
        elif fmt == "xml":
            df = self._read_xml(source_path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        # Add audit metadata columns (same as structured ingestion)
        df = add_metadata_columns(df, source_file=source_path, source_format=fmt)

        # Overwrite mode — idempotent, same as structured Bronze tables
        write_delta_table(
            df=df,
            catalog=CATALOG,
            schema=SCHEMA,
            table=table_name,
            mode="overwrite",
        )

        row_count = df.count()
        print(f"    Rows: {row_count:,}")
        print(f"    Table: {CATALOG}.{SCHEMA}.{table_name}")

    def _read_json(self, source_path: str):
        """Read JSON files preserving nested structure.

        TRAINEE NOTE — JSON reading options explained:
          multiLine=true : A single JSON record can span multiple lines.
                           Without this, Spark expects one complete JSON
                           object per line (JSON Lines / NDJSON format).
                           Most "pretty-printed" JSON files require multiLine.

          mode=PERMISSIVE : If a JSON record is malformed (e.g. missing a
                            closing brace), Spark does NOT crash. Instead it
                            stores the raw malformed text in _corrupt_record
                            and continues processing other records.
                            Alternative modes:
                              DROPMALFORMED — silently drop bad records (data loss risk)
                              FAILFAST      — throw an exception on first bad record

          columnNameOfCorruptRecord="_corrupt_record" : The column where
                            malformed JSON text is placed. You can query this
                            column in Bronze to find and fix source data issues.

        Nested JSON is automatically "flattened" into columns by Spark:
          {"policy": {"id": "001", "state": "CA"}}
          → columns: policy.id, policy.state (dot notation)
        These nested column names will be standardised to snake_case in Silver.
        """
        return (
            self.spark.read.option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(source_path)
        )

    def _read_xml(self, source_path: str, row_tag: str = "record"):
        """
        Read XML files using spark-xml library.
        Requires: com.databricks:spark-xml_2.12:0.17.0

        TRAINEE NOTE — XML reading:
        XML does not have a native Spark reader — it requires the third-party
        spark-xml library (maintained by Databricks). Add the Maven coordinate
        to your cluster's Libraries tab or use --packages in spark-submit.

        rowTag: The XML element name that represents one data row.
          e.g. for <records><record>...</record></records>, rowTag="record"
          Spark will read each <record> element as one DataFrame row.

        mode=PERMISSIVE: Same as JSON — continue on malformed XML.

        XML namespaces and attributes are handled automatically by spark-xml.
        Attributes become columns prefixed with "_VALUE" by default.
        """
        return (
            self.spark.read.format("com.databricks.spark.xml")
            .option("rowTag", row_tag)
            .option("mode", "PERMISSIVE")
            .load(source_path)
        )

    def ingest_fema_api_json(self, api_response_path: str, table_name: str):
        """
        Ingest FEMA API JSON response which has a nested structure:
        { "metadata": {...}, "FimaNfipClaims": [{...}, {...}, ...] }

        TRAINEE NOTE — Handling wrapped/nested API responses
        REST APIs often wrap the actual data records inside a top-level object:
          {
            "metadata": { "total": 2700000, "page": 1 },
            "FimaNfipClaims": [
              { "policyId": "001", "claimAmount": 50000 },
              { "policyId": "002", "claimAmount": 12000 }
            ]
          }

        If we read this directly, we get ONE row with a "FimaNfipClaims" column
        that contains an array of 2.7 million objects — not useful for analytics.

        We need to EXPLODE the array into individual rows:
          F.explode(F.col("FimaNfipClaims")) → one row per claim

        Then select("record.*") expands all nested fields into top-level columns.

        How we detect the dataset key dynamically:
          We look at all top-level columns in the raw response and pick the one
          that is NOT "metadata" or "_corrupt_record". This makes the function
          reusable for different FEMA endpoints without hardcoding the key name.

        Result: A flat DataFrame where each row = one FEMA claim/policy record,
        equivalent to what we'd get from a bulk CSV download.
        """
        print(f"\n  Ingesting FEMA API JSON: {table_name}")

        # Read the entire API response as a single-row DataFrame
        raw_df = self.spark.read.option("multiLine", "true").json(api_response_path)

        # The FEMA API wraps records in a top-level key — explode it
        # Detect the dataset key (not metadata or corrupt record column)
        dataset_cols = [c for c in raw_df.columns if c != "metadata" and c != "_corrupt_record"]

        if dataset_cols:
            dataset_key = dataset_cols[0]
            # F.explode: converts array column into multiple rows (one per array element)
            df = raw_df.select(F.explode(F.col(dataset_key)).alias("record"))
            # select("record.*"): expands the struct into individual columns
            df = df.select("record.*")
        else:
            # Fallback: if structure is unexpected, just use raw data as-is
            df = raw_df

        df = add_metadata_columns(df, source_file=api_response_path, source_format="json_api")

        write_delta_table(
            df=df,
            catalog=CATALOG,
            schema=SCHEMA,
            table=table_name,
            mode="overwrite",
        )

        print(f"    Rows: {df.count():,}")
        print(f"    Table: {CATALOG}.{SCHEMA}.{table_name}")
