"""
Schema definitions for all layers of the Insurance Data Platform.
Bronze: raw string schemas for CSV, typed for JSON/XML
Silver: cleansed typed schemas per LOB
Gold: dimension and fact table schemas

TRAINEE GUIDE — What are schemas and why do we define them here?

A schema is a blueprint that tells PySpark what columns a DataFrame has
and what TYPE each column is (string, integer, date, decimal, etc.).

Why define schemas explicitly (instead of letting Spark infer them)?
  1. PERFORMANCE: Schema inference requires Spark to scan the entire dataset
     TWICE — once to guess types, once to read data. Providing a schema
     avoids the inference scan, speeding up reads significantly.
  2. CORRECTNESS: Inference can guess wrong. A column with "001", "002"
     might be inferred as integer (losing leading zeros) when it should
     be a string code.
  3. DOCUMENTATION: Schemas serve as a data dictionary — you can see at a
     glance what columns and types each table has.
  4. CONSISTENCY: All team members reference the same schema definitions
     instead of writing ad-hoc schemas in their own code.

PySpark schema building blocks:
  StructType     : Represents a table/record — contains a list of fields
  StructField    : One column — has a name, type, and nullable flag
  StringType     : Text/string data (e.g., "John", "PROP-001")
  IntegerType    : Whole numbers (e.g., 2024, 3, 100)
  DoubleType     : Floating-point numbers (e.g., 40.7128, -74.0060)
  DecimalType    : Fixed-precision numbers for money (e.g., 1500.00)
                   DecimalType(18,2) = up to 18 digits, 2 after decimal
                   ALWAYS use Decimal (not Double) for financial amounts to
                   avoid floating-point rounding errors ($1.10 + $2.20 ≠ $3.30 with floats!)
  DateType       : Calendar dates (e.g., 2024-01-15)
  TimestampType  : Date + time (e.g., 2024-01-15 10:30:00)
  BooleanType    : True/False values
  ArrayType      : A list/array of values (e.g., list of claims in a JSON record)
  MapType        : A key-value dictionary (e.g., {"city": "Miami", "state": "FL"})

The nullable flag (True/False, 3rd argument of StructField):
  True  = the column CAN contain null values (default for most columns)
  False = the column MUST NOT be null (used for primary keys like policy_id)

Schema organisation in this file:
  BronzeSchemas  → Raw data schemas (strings for CSV, native types for JSON/XML)
  SilverSchemas  → Cleansed, typed schemas per LOB (after type casting in Silver)
  GoldSchemas    → Dimension and fact table schemas (star schema design)
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    BooleanType,
    ArrayType,
    MapType,
)

# =============================================================================
# BRONZE SCHEMAS — Raw ingestion schemas
# =============================================================================


class BronzeSchemas:
    """Raw landing schemas. CSV uses all-string; JSON/XML preserve native types.

    TRAINEE NOTE — Why are ALL CSV columns StringType()?
    In the Bronze layer we follow a "schema-on-read" approach:
      - CSV data is read with EVERY column as a string, regardless of whether
        the value looks like a number, date, or boolean.
      - Type conversion happens LATER in the Silver layer (transform_*.py).

    Why defer type casting?
      1. CSV is a text format — everything is inherently a string on disk.
      2. Type inference is unreliable. A column like "001" could be an integer
         (1) or a string code ("001"). Keeping it as a string preserves the
         leading zeros.
      3. If inference fails on one bad record (e.g. "N/A" in a numeric column),
         the entire read can fail. Strings always succeed.
      4. It separates concerns: Bronze = land as-is; Silver = apply business rules.

    For JSON and XML, native types ARE preserved because these formats embed
    type information (numbers are numbers, booleans are booleans in JSON).
    JSON schemas also use MapType and ArrayType for nested structures.
    """

    # -------------------------------------------------------------------------
    # Property — FEMA NFIP Claims (CSV raw: all strings)
    # TRAINEE NOTE: These column names match the FEMA API field names exactly
    # (camelCase). They will be standardised to snake_case in Silver.
    # -------------------------------------------------------------------------
    PROPERTY_CLAIMS_CSV = StructType(
        [
            StructField("agricultureStructureIndicator", StringType(), True),
            StructField("asOfDate", StringType(), True),
            StructField("basementEnclosureCrawlspaceType", StringType(), True),
            StructField("reportedCity", StringType(), True),
            StructField("condominiumIndicator", StringType(), True),
            StructField("countyCode", StringType(), True),
            StructField("communityRatingSystemDiscount", StringType(), True),
            StructField("dateOfLoss", StringType(), True),
            StructField("elevatedBuildingIndicator", StringType(), True),
            StructField("elevationCertificateIndicator", StringType(), True),
            StructField("elevationDifference", StringType(), True),
            StructField("floodZone", StringType(), True),
            StructField("houseWorship", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("locationOfContents", StringType(), True),
            StructField("lowestAdjacentGrade", StringType(), True),
            StructField("lowestFloorElevation", StringType(), True),
            StructField("numberOfFloorsInTheInsuredBuilding", StringType(), True),
            StructField("nonProfitIndicator", StringType(), True),
            StructField("obstructionType", StringType(), True),
            StructField("occupancyType", StringType(), True),
            StructField("originalConstructionDate", StringType(), True),
            StructField("originalNBDate", StringType(), True),
            StructField("amountPaidOnBuildingClaim", StringType(), True),
            StructField("amountPaidOnContentsClaim", StringType(), True),
            StructField("amountPaidOnIncreasedCostOfComplianceClaim", StringType(), True),
            StructField("postFIRMConstructionIndicator", StringType(), True),
            StructField("rateMethod", StringType(), True),
            StructField("smallBusinessIndicatorBuilding", StringType(), True),
            StructField("state", StringType(), True),
            StructField("totalBuildingInsuranceCoverage", StringType(), True),
            StructField("totalContentsInsuranceCoverage", StringType(), True),
            StructField("yearOfLoss", StringType(), True),
            StructField("reportedZipCode", StringType(), True),
            StructField("primaryResidence", StringType(), True),
            StructField("id", StringType(), True),
        ]
    )

    # -------------------------------------------------------------------------
    # Property — FEMA NFIP Policies (CSV raw)
    # -------------------------------------------------------------------------
    PROPERTY_POLICIES_CSV = StructType(
        [
            StructField("agricultureStructureIndicator", StringType(), True),
            StructField("basementEnclosureCrawlspaceType", StringType(), True),
            StructField("cancellationDateOfFloodPolicy", StringType(), True),
            StructField("condominiumIndicator", StringType(), True),
            StructField("countyCode", StringType(), True),
            StructField("crsClassCode", StringType(), True),
            StructField("deductibleAmountInBuildingCoverage", StringType(), True),
            StructField("deductibleAmountInContentsCoverage", StringType(), True),
            StructField("elevatedBuildingIndicator", StringType(), True),
            StructField("elevationCertificateIndicator", StringType(), True),
            StructField("elevationDifference", StringType(), True),
            StructField("federalPolicyFee", StringType(), True),
            StructField("floodZone", StringType(), True),
            StructField("hfiaaSurcharge", StringType(), True),
            StructField("houseWorship", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("locationOfContents", StringType(), True),
            StructField("lowestAdjacentGrade", StringType(), True),
            StructField("lowestFloorElevation", StringType(), True),
            StructField("numberOfFloorsInTheInsuredBuilding", StringType(), True),
            StructField("nonProfitIndicator", StringType(), True),
            StructField("obstructionType", StringType(), True),
            StructField("occupancyType", StringType(), True),
            StructField("originalConstructionDate", StringType(), True),
            StructField("originalNBDate", StringType(), True),
            StructField("policyCount", StringType(), True),
            StructField("policyEffectiveDate", StringType(), True),
            StructField("policyTerminationDate", StringType(), True),
            StructField("policyTermIndicator", StringType(), True),
            StructField("postFIRMConstructionIndicator", StringType(), True),
            StructField("primaryResidence", StringType(), True),
            StructField("propertyState", StringType(), True),
            StructField("rateMethod", StringType(), True),
            StructField("regularEmergencyProgramIndicator", StringType(), True),
            StructField("reportedCity", StringType(), True),
            StructField("reportedZipCode", StringType(), True),
            StructField("smallBusinessIndicatorBuilding", StringType(), True),
            StructField("totalBuildingInsuranceCoverage", StringType(), True),
            StructField("totalContentsInsuranceCoverage", StringType(), True),
            StructField("totalInsurancePremiumOfThePolicy", StringType(), True),
            StructField("id", StringType(), True),
        ]
    )

    # -------------------------------------------------------------------------
    # Property — JSON (semi-structured from FEMA API)
    # TRAINEE NOTE: JSON schemas use complex types:
    #   MapType(StringType(), StringType()) = a key-value dictionary (like Python dict)
    #   ArrayType(MapType(...)) = a list of dictionaries (like a list of records)
    # The FEMA API wraps claim records inside a "FimaNfipClaims" array, with
    # a separate "metadata" map containing pagination info (total, skip, top).
    # -------------------------------------------------------------------------
    PROPERTY_CLAIMS_JSON = StructType(
        [
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("FimaNfipClaims", ArrayType(MapType(StringType(), StringType())), True),
        ]
    )

    # -------------------------------------------------------------------------
    # Workers' Compensation — CAS Schedule P + Synthetic
    # TRAINEE NOTE: WC data includes BOTH policy and claim fields in one CSV.
    # This is common in legacy systems where policies and claims are combined
    # in a single flat file. Silver will split them into separate tables.
    # Key WC fields: employer_name, payroll_amount, class_code, experience_mod_factor
    # -------------------------------------------------------------------------
    WORKERS_COMP_CSV = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("employer_name", StringType(), True),
            StructField("fein", StringType(), True),
            StructField("naics_code", StringType(), True),
            StructField("class_code", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("payroll_amount", StringType(), True),
            StructField("experience_mod_factor", StringType(), True),
            StructField("premium_amount", StringType(), True),
            StructField("claim_id", StringType(), True),
            StructField("date_of_injury", StringType(), True),
            StructField("injury_type", StringType(), True),
            StructField("body_part", StringType(), True),
            StructField("nature_of_injury", StringType(), True),
            StructField("cause_of_injury", StringType(), True),
            StructField("employee_age", StringType(), True),
            StructField("employee_gender", StringType(), True),
            StructField("medical_paid", StringType(), True),
            StructField("indemnity_paid", StringType(), True),
            StructField("total_incurred", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("return_to_work_date", StringType(), True),
            StructField("insured_name", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
        ]
    )

    # TRAINEE NOTE: WC JSON uses nested structures to group related fields:
    #   employer_info    → MapType: {"name": "Acme Corp", "fein": "12-3456789"}
    #   coverage_details → MapType: {"class_code": "8810", "mod_factor": "0.95"}
    #   claims           → ArrayType(MapType): list of claim records
    #   payroll_details  → ArrayType(MapType): list of payroll entries by class
    # These nested structures will be "flattened" into individual columns in Silver.
    WORKERS_COMP_JSON = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("employer_info", MapType(StringType(), StringType()), True),
            StructField("coverage_details", MapType(StringType(), StringType()), True),
            StructField("claims", ArrayType(MapType(StringType(), StringType())), True),
            StructField("payroll_details", ArrayType(MapType(StringType(), StringType())), True),
        ]
    )

    # -------------------------------------------------------------------------
    # Auto Insurance
    # TRAINEE NOTE: Auto CSV includes vehicle-specific fields (VIN, make, model,
    # year) and driver info (insured_dob, insured_gender) used in premium rating.
    # Claims include accident details (fault, vehicles involved, bodily injuries).
    # -------------------------------------------------------------------------
    AUTO_CLAIMS_CSV = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("insured_name", StringType(), True),
            StructField("insured_dob", StringType(), True),
            StructField("insured_gender", StringType(), True),
            StructField("vehicle_year", StringType(), True),
            StructField("vehicle_make", StringType(), True),
            StructField("vehicle_model", StringType(), True),
            StructField("vin", StringType(), True),
            StructField("garage_state", StringType(), True),
            StructField("garage_zip", StringType(), True),
            StructField("coverage_type", StringType(), True),
            StructField("liability_limit", StringType(), True),
            StructField("deductible_amount", StringType(), True),
            StructField("premium_amount", StringType(), True),
            StructField("claim_id", StringType(), True),
            StructField("date_of_accident", StringType(), True),
            StructField("accident_description", StringType(), True),
            StructField("fault_indicator", StringType(), True),
            StructField("number_vehicles_involved", StringType(), True),
            StructField("bodily_injury_count", StringType(), True),
            StructField("property_damage_amount", StringType(), True),
            StructField("total_claim_amount", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
        ]
    )

    AUTO_CLAIMS_JSON = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("insured_info", MapType(StringType(), StringType()), True),
            StructField("vehicle_info", MapType(StringType(), StringType()), True),
            StructField("coverage", MapType(StringType(), StringType()), True),
            StructField("claims", ArrayType(MapType(StringType(), StringType())), True),
            StructField("telematics", MapType(StringType(), StringType()), True),
        ]
    )

    # -------------------------------------------------------------------------
    # General Liability
    # TRAINEE NOTE: GL covers businesses against third-party claims. Key fields:
    #   business_type, naics_code = what the business does
    #   occurrence_limit = max payout per single event
    #   aggregate_limit = max payout per policy year (usually 2× occurrence)
    #   revenue = annual revenue, used as the exposure base for premium rating
    # -------------------------------------------------------------------------
    GENERAL_LIABILITY_CSV = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("insured_name", StringType(), True),
            StructField("business_type", StringType(), True),
            StructField("naics_code", StringType(), True),
            StructField("class_code", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("revenue", StringType(), True),
            StructField("premium_amount", StringType(), True),
            StructField("occurrence_limit", StringType(), True),
            StructField("aggregate_limit", StringType(), True),
            StructField("deductible_amount", StringType(), True),
            StructField("claim_id", StringType(), True),
            StructField("date_of_occurrence", StringType(), True),
            StructField("claim_type", StringType(), True),
            StructField("claim_description", StringType(), True),
            StructField("claimant_name", StringType(), True),
            StructField("incurred_amount", StringType(), True),
            StructField("paid_amount", StringType(), True),
            StructField("reserve_amount", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("litigation_flag", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
        ]
    )

    # TRAINEE NOTE: XML schema uses nested StructType for hierarchical data.
    # StructType inside StructType = a nested object (like JSON objects).
    # "insured" is a nested struct containing name, business_type, address, etc.
    # "coverage" is another nested struct with limits and premium.
    # "claims" is an ArrayType of structs — each claim is a separate struct in the list.
    # This mirrors how XML organises data into parent/child elements:
    #   <policy><insured><name>Acme</name></insured><claims><claim>...</claim></claims></policy>
    GENERAL_LIABILITY_XML = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField(
                "insured",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("business_type", StringType(), True),
                        StructField("naics_code", StringType(), True),
                        StructField("address", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("zip", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "coverage",
                StructType(
                    [
                        StructField("occurrence_limit", StringType(), True),
                        StructField("aggregate_limit", StringType(), True),
                        StructField("deductible", StringType(), True),
                        StructField("premium", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "claims",
                ArrayType(
                    StructType(
                        [
                            StructField("claim_id", StringType(), True),
                            StructField("date_of_occurrence", StringType(), True),
                            StructField("claim_type", StringType(), True),
                            StructField("amount", StringType(), True),
                            StructField("status", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )

    # -------------------------------------------------------------------------
    # Umbrella Insurance
    # TRAINEE NOTE: Umbrella policies provide EXCESS liability coverage that
    # sits on top of other policies (Auto, GL, WC). Key fields:
    #   umbrella_limit = additional coverage above underlying policy limits
    #   retention_amount = self-insured retention (the deductible for umbrella)
    #   underlying_*_policy_id = FK to the policies this umbrella extends
    #   underlying_line = which underlying LOB's claim triggered the umbrella
    # -------------------------------------------------------------------------
    UMBRELLA_CSV = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("expiration_date", StringType(), True),
            StructField("insured_name", StringType(), True),
            StructField("insured_type", StringType(), True),
            StructField("umbrella_limit", StringType(), True),
            StructField("retention_amount", StringType(), True),
            StructField("premium_amount", StringType(), True),
            StructField("underlying_auto_policy_id", StringType(), True),
            StructField("underlying_gl_policy_id", StringType(), True),
            StructField("underlying_wc_policy_id", StringType(), True),
            StructField("underlying_employers_liability_id", StringType(), True),
            StructField("claim_id", StringType(), True),
            StructField("date_of_occurrence", StringType(), True),
            StructField("underlying_line", StringType(), True),
            StructField("underlying_claim_id", StringType(), True),
            StructField("excess_amount", StringType(), True),
            StructField("total_claim_amount", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip_code", StringType(), True),
        ]
    )

    UMBRELLA_JSON = StructType(
        [
            StructField("policy_id", StringType(), True),
            StructField("policy_number", StringType(), True),
            StructField("insured_info", MapType(StringType(), StringType()), True),
            StructField("umbrella_coverage", MapType(StringType(), StringType()), True),
            StructField("underlying_policies", ArrayType(MapType(StringType(), StringType())), True),
            StructField("claims", ArrayType(MapType(StringType(), StringType())), True),
        ]
    )


# =============================================================================
# SILVER SCHEMAS — Cleansed and typed
# =============================================================================


class SilverSchemas:
    """Cleansed, typed, standardized schemas per LOB.

    TRAINEE NOTE — How Silver schemas differ from Bronze:
    1. TYPED columns: StringType → DateType, DecimalType, IntegerType, BooleanType
       Bronze stores everything as strings; Silver applies the correct types.
    2. SNAKE_CASE names: Bronze keeps original names (camelCase from FEMA);
       Silver standardises to snake_case for SQL compatibility.
    3. NULLABLE flags: Primary keys (policy_id, claim_id) are NOT nullable (False).
       Most other columns ARE nullable (True) — missing data is expected.
    4. FINANCIAL columns use DecimalType(18, 2) — NOT DoubleType — to avoid
       floating-point rounding errors in monetary calculations.
    5. DQ columns added: _is_valid (BooleanType) and _dq_issues (StringType)
       flag rows that failed data quality checks (see data_quality.py).
    6. _ingestion_timestamp (TimestampType) carried from Bronze for audit trail.

    There are 10 Silver schemas: 2 per LOB (policies + claims) × 5 LOBs.
    """

    # TRAINEE NOTE — Property policies Silver schema:
    # Notice the mix of types: StringType for IDs/names, DateType for dates,
    # DecimalType(18,2) for monetary amounts, DoubleType for GPS coordinates,
    # IntegerType for counts, BooleanType for flags.
    # The last 3 columns (_is_valid, _dq_issues, _ingestion_timestamp) are
    # system columns present in EVERY Silver table.
    PROPERTY_POLICIES = StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("insured_name", StringType(), True),
            StructField("property_state", StringType(), True),
            StructField("reported_city", StringType(), True),
            StructField("reported_zip_code", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("occupancy_type", StringType(), True),
            StructField("construction_type", StringType(), True),
            StructField("year_built", IntegerType(), True),
            StructField("number_of_floors", IntegerType(), True),
            StructField("flood_zone", StringType(), True),
            StructField("total_building_coverage", DecimalType(18, 2), True),
            StructField("total_contents_coverage", DecimalType(18, 2), True),
            StructField("building_value", DecimalType(18, 2), True),
            StructField("contents_value", DecimalType(18, 2), True),
            StructField("premium_amount", DecimalType(18, 2), True),
            StructField("deductible_amount", DecimalType(18, 2), True),
            StructField("primary_residence", BooleanType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    PROPERTY_CLAIMS = StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("date_of_loss", DateType(), True),
            StructField("year_of_loss", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("reported_city", StringType(), True),
            StructField("reported_zip_code", StringType(), True),
            StructField("cause_of_damage", StringType(), True),
            StructField("amount_paid_building", DecimalType(18, 2), True),
            StructField("amount_paid_contents", DecimalType(18, 2), True),
            StructField("building_damage_amount", DecimalType(18, 2), True),
            StructField("contents_damage_amount", DecimalType(18, 2), True),
            StructField("claim_status", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    WORKERS_COMP_POLICIES = StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("employer_name", StringType(), True),
            StructField("fein", StringType(), True),
            StructField("naics_code", StringType(), True),
            StructField("class_code", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("payroll_amount", DecimalType(18, 2), True),
            StructField("experience_mod_factor", DecimalType(6, 3), True),
            StructField("premium_amount", DecimalType(18, 2), True),
            StructField("insured_name", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    WORKERS_COMP_CLAIMS = StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("date_of_injury", DateType(), True),
            StructField("injury_type", StringType(), True),
            StructField("body_part", StringType(), True),
            StructField("nature_of_injury", StringType(), True),
            StructField("cause_of_injury", StringType(), True),
            StructField("employee_age", IntegerType(), True),
            StructField("employee_gender", StringType(), True),
            StructField("medical_paid", DecimalType(18, 2), True),
            StructField("indemnity_paid", DecimalType(18, 2), True),
            StructField("total_incurred", DecimalType(18, 2), True),
            StructField("claim_status", StringType(), True),
            StructField("return_to_work_date", DateType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    AUTO_POLICIES = StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("insured_name", StringType(), True),
            StructField("insured_dob", DateType(), True),
            StructField("insured_gender", StringType(), True),
            StructField("vehicle_year", IntegerType(), True),
            StructField("vehicle_make", StringType(), True),
            StructField("vehicle_model", StringType(), True),
            StructField("vin", StringType(), True),
            StructField("garage_state", StringType(), True),
            StructField("garage_zip", StringType(), True),
            StructField("coverage_type", StringType(), True),
            StructField("liability_limit", DecimalType(18, 2), True),
            StructField("deductible_amount", DecimalType(18, 2), True),
            StructField("premium_amount", DecimalType(18, 2), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    AUTO_CLAIMS = StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("date_of_accident", DateType(), True),
            StructField("accident_description", StringType(), True),
            StructField("fault_indicator", StringType(), True),
            StructField("number_vehicles_involved", IntegerType(), True),
            StructField("bodily_injury_count", IntegerType(), True),
            StructField("property_damage_amount", DecimalType(18, 2), True),
            StructField("total_claim_amount", DecimalType(18, 2), True),
            StructField("claim_status", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    GENERAL_LIABILITY_POLICIES = StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("insured_name", StringType(), True),
            StructField("business_type", StringType(), True),
            StructField("naics_code", StringType(), True),
            StructField("class_code", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("revenue", DecimalType(18, 2), True),
            StructField("premium_amount", DecimalType(18, 2), True),
            StructField("occurrence_limit", DecimalType(18, 2), True),
            StructField("aggregate_limit", DecimalType(18, 2), True),
            StructField("deductible_amount", DecimalType(18, 2), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    GENERAL_LIABILITY_CLAIMS = StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("date_of_occurrence", DateType(), True),
            StructField("claim_type", StringType(), True),
            StructField("claim_description", StringType(), True),
            StructField("claimant_name", StringType(), True),
            StructField("incurred_amount", DecimalType(18, 2), True),
            StructField("paid_amount", DecimalType(18, 2), True),
            StructField("reserve_amount", DecimalType(18, 2), True),
            StructField("claim_status", StringType(), True),
            StructField("litigation_flag", BooleanType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    UMBRELLA_POLICIES = StructType(
        [
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("effective_date", DateType(), True),
            StructField("expiration_date", DateType(), True),
            StructField("insured_name", StringType(), True),
            StructField("insured_type", StringType(), True),
            StructField("umbrella_limit", DecimalType(18, 2), True),
            StructField("retention_amount", DecimalType(18, 2), True),
            StructField("premium_amount", DecimalType(18, 2), True),
            StructField("underlying_auto_policy_id", StringType(), True),
            StructField("underlying_gl_policy_id", StringType(), True),
            StructField("underlying_wc_policy_id", StringType(), True),
            StructField("agent_id", StringType(), True),
            StructField("agent_name", StringType(), True),
            StructField("underwriter", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )

    UMBRELLA_CLAIMS = StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), True),
            StructField("date_of_occurrence", DateType(), True),
            StructField("underlying_line", StringType(), True),
            StructField("underlying_claim_id", StringType(), True),
            StructField("excess_amount", DecimalType(18, 2), True),
            StructField("total_claim_amount", DecimalType(18, 2), True),
            StructField("claim_status", StringType(), True),
            StructField("_is_valid", BooleanType(), True),
            StructField("_dq_issues", StringType(), True),
            StructField("_ingestion_timestamp", TimestampType(), True),
        ]
    )


# =============================================================================
# GOLD SCHEMAS — Dimension and Fact tables
# =============================================================================


class GoldSchemas:
    """Normalized star schema for the Gold layer.

    TRAINEE NOTE — Star Schema Design (Kimball methodology)
    The Gold layer follows the STAR SCHEMA pattern — the most common design
    for data warehouses and analytics. It consists of:

    DIMENSION TABLES (the "points" of the star):
      Store descriptive attributes (WHO, WHAT, WHERE, WHEN):
        dim_date             : Calendar dates with fiscal year, holiday flags
        dim_line_of_business : The 5 LOBs (Property, Auto, WC, GL, Umbrella)
        dim_insured          : Policyholders (people or businesses)
        dim_location         : Addresses and geographic info
        dim_agent            : Insurance agents/brokers
        dim_coverage         : Types of coverage per LOB
        dim_policy           : The central policy dimension (links to all others)
        dim_claim            : Claim events with cause and status

    FACT TABLES (the "center" of the star):
      Store measurable events with numeric metrics:
        fact_premium           : Premium amounts (written, earned, ceded, net)
        fact_claim_transaction : Claim payments and reserves
        fact_policy_transaction: Policy lifecycle events (new, renewal, cancel)

    Key design decisions:
      - Surrogate keys: Every dimension has a surrogate_key (MD5 hash) as PK
        and keeps the natural key (e.g. policy_id) for traceability.
      - Date keys: Integer YYYYMMDD format (e.g. 20240115) for fast joins with dim_date.
      - DecimalType(18,2): All monetary amounts use fixed-precision decimal.
      - Facts contain ONLY keys (FKs to dimensions) + numeric measures.
        No descriptive text belongs in a fact table — that goes in dimensions.

    Why Star Schema?
      1. Simple queries: Business users can write SQL with straightforward JOINs.
      2. Fast aggregations: Fact tables are narrow (few columns, many rows).
      3. BI tool friendly: Tools like Power BI, Tableau, Looker expect star schemas.
      4. Flexible: Add new dimensions or facts without restructuring existing ones.
    """

    # TRAINEE NOTE — dim_date: A "role-playing" dimension
    # The same dim_date table is joined multiple times in queries using
    # different date columns: effective_date_key, expiration_date_key,
    # date_of_loss_key, etc. Each join uses the same dim_date table but
    # via a different FK — this is called "role-playing dimensions".
    # date_key uses integer YYYYMMDD format (e.g. 20240115) for fast joins.
    DIM_DATE = StructType(
        [
            StructField("date_key", IntegerType(), False),
            StructField("full_date", DateType(), False),
            StructField("year", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("month_name", StringType(), True),
            StructField("day", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
            StructField("is_holiday", BooleanType(), True),
            StructField("fiscal_year", IntegerType(), True),
            StructField("fiscal_quarter", IntegerType(), True),
        ]
    )

    # TRAINEE NOTE — dim_line_of_business: A small "reference" dimension
    # Only 5 rows (one per LOB). lob_key is the surrogate key; lob_code
    # (PROP, WC, AUTO, GL, UMB) is the natural key.
    # naic_code = National Association of Insurance Commissioners code,
    # the standard industry classification for insurance lines.
    DIM_LINE_OF_BUSINESS = StructType(
        [
            StructField("lob_key", StringType(), False),
            StructField("lob_code", StringType(), False),
            StructField("lob_name", StringType(), True),
            StructField("lob_category", StringType(), True),
            StructField("regulatory_body", StringType(), True),
            StructField("naic_code", StringType(), True),
        ]
    )

    # TRAINEE NOTE — dim_insured: Master dimension for all policyholders
    # Combines insured parties from ALL 5 LOBs into one unified table.
    # insured_type: "Individual" (person) vs "Commercial" (business)
    # ein = Employer Identification Number (like SSN but for businesses)
    # sic_code / naics_code = industry classification codes
    DIM_INSURED = StructType(
        [
            StructField("insured_key", StringType(), False),
            StructField("insured_id", StringType(), False),
            StructField("insured_name", StringType(), True),
            StructField("insured_type", StringType(), True),
            StructField("date_of_birth", DateType(), True),
            StructField("gender", StringType(), True),
            StructField("ein", StringType(), True),
            StructField("sic_code", StringType(), True),
            StructField("naics_code", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("employee_count", IntegerType(), True),
            StructField("annual_revenue", DecimalType(18, 2), True),
        ]
    )

    # TRAINEE NOTE — dim_location: Master location dimension
    # Combines addresses from all LOBs. territory_code is used in insurance
    # rating (different territories have different base rates).
    # flood_zone comes from FEMA flood maps (affects Property insurance pricing).
    DIM_LOCATION = StructType(
        [
            StructField("location_key", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("address_line1", StringType(), True),
            StructField("address_line2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("county", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("state_name", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("territory_code", StringType(), True),
            StructField("flood_zone", StringType(), True),
        ]
    )

    # TRAINEE NOTE — dim_agent: Insurance agents/brokers who sell policies
    # commission_tier determines the agent's commission percentage on premiums.
    # license_number is the state-issued insurance license.
    DIM_AGENT = StructType(
        [
            StructField("agent_key", StringType(), False),
            StructField("agent_id", StringType(), False),
            StructField("agent_name", StringType(), True),
            StructField("agency_name", StringType(), True),
            StructField("agency_code", StringType(), True),
            StructField("license_number", StringType(), True),
            StructField("state_code", StringType(), True),
            StructField("commission_tier", StringType(), True),
        ]
    )

    # TRAINEE NOTE — dim_coverage: Types of insurance coverage
    # Each LOB has its own coverage types (e.g. "Comprehensive" for Auto,
    # "Building" for Property). lob_key links coverage back to its LOB.
    # is_mandatory indicates whether this coverage is legally required.
    DIM_COVERAGE = StructType(
        [
            StructField("coverage_key", StringType(), False),
            StructField("coverage_id", StringType(), False),
            StructField("coverage_code", StringType(), True),
            StructField("coverage_name", StringType(), True),
            StructField("coverage_type", StringType(), True),
            StructField("coverage_description", StringType(), True),
            StructField("lob_key", StringType(), True),
            StructField("is_mandatory", BooleanType(), True),
        ]
    )

    # TRAINEE NOTE — dim_policy: The CENTRAL dimension of the star schema
    # This is the largest and most important dimension. It unifies policies
    # from ALL 5 LOBs into a single table with consistent structure.
    # Foreign keys link to other dimensions:
    #   lob_key            → dim_line_of_business (which LOB)
    #   insured_key        → dim_insured (who is the policyholder)
    #   agent_key          → dim_agent (who sold the policy)
    #   location_key       → dim_location (where is the risk)
    #   effective_date_key → dim_date (when does coverage start)
    #   expiration_date_key→ dim_date (when does coverage end)
    # These FKs allow analysts to "slice and dice" policies by any dimension.
    DIM_POLICY = StructType(
        [
            StructField("policy_key", StringType(), False),
            StructField("policy_id", StringType(), False),
            StructField("policy_number", StringType(), True),
            StructField("lob_key", StringType(), True),
            StructField("insured_key", StringType(), True),
            StructField("agent_key", StringType(), True),
            StructField("location_key", StringType(), True),
            StructField("effective_date_key", IntegerType(), True),
            StructField("expiration_date_key", IntegerType(), True),
            StructField("policy_status", StringType(), True),
            StructField("policy_term_months", IntegerType(), True),
            StructField("underwriter", StringType(), True),
            StructField("risk_tier", StringType(), True),
            StructField("is_renewal", BooleanType(), True),
            StructField("original_policy_id", StringType(), True),
        ]
    )

    # TRAINEE NOTE — dim_claim: Master claim dimension
    # Links to dim_policy via policy_key (which policy does this claim belong to).
    # date_of_loss_key / date_reported_key are date FKs for time-based analysis.
    # catastrophe_code identifies if the claim is part of a declared catastrophe
    # (e.g. a hurricane) — catastrophe claims are analysed separately.
    DIM_CLAIM = StructType(
        [
            StructField("claim_key", StringType(), False),
            StructField("claim_id", StringType(), False),
            StructField("claim_number", StringType(), True),
            StructField("policy_key", StringType(), True),
            StructField("claimant_name", StringType(), True),
            StructField("claimant_type", StringType(), True),
            StructField("date_of_loss_key", IntegerType(), True),
            StructField("date_reported_key", IntegerType(), True),
            StructField("cause_of_loss", StringType(), True),
            StructField("claim_type", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("litigation_flag", BooleanType(), True),
            StructField("catastrophe_code", StringType(), True),
            StructField("adjuster_name", StringType(), True),
        ]
    )

    # TRAINEE NOTE — fact_premium: Premium revenue fact table
    # Grain: one row per policy-coverage premium record.
    # Contains ONLY foreign keys (to dimensions) + numeric measures:
    #   written_premium  : Full annual premium billed to the insured
    #   earned_premium   : Portion of premium "earned" as time passes
    #                      (if a 12-month policy is 6 months in, 50% is earned)
    #   ceded_premium    : Portion passed to reinsurers (risk transfer, ~15%)
    #   net_premium      : written - ceded (what the insurer keeps)
    #   commission_amount: Agent's commission (~12% of written premium)
    #   tax_amount       : State premium taxes (~3%)
    FACT_PREMIUM = StructType(
        [
            StructField("premium_key", StringType(), False),
            StructField("policy_key", StringType(), True),
            StructField("coverage_key", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("lob_key", StringType(), True),
            StructField("location_key", StringType(), True),
            StructField("written_premium", DecimalType(18, 2), True),
            StructField("earned_premium", DecimalType(18, 2), True),
            StructField("ceded_premium", DecimalType(18, 2), True),
            StructField("net_premium", DecimalType(18, 2), True),
            StructField("deductible_amount", DecimalType(18, 2), True),
            StructField("coverage_limit", DecimalType(18, 2), True),
            StructField("commission_amount", DecimalType(18, 2), True),
            StructField("tax_amount", DecimalType(18, 2), True),
        ]
    )

    # TRAINEE NOTE — fact_claim_transaction: Claim payment activity
    # Grain: one row per claim transaction (payment, reserve change, recovery).
    # transaction_type: "Payment" (money paid out), "Reserve" (money set aside),
    #                   "Recovery" (money recovered via subrogation/salvage)
    # Key insurance metric: total_incurred = paid + reserve + expense - recovery
    # is_final_payment flags the last payment on a claim (claim is settling).
    FACT_CLAIM_TRANSACTION = StructType(
        [
            StructField("claim_txn_key", StringType(), False),
            StructField("claim_key", StringType(), True),
            StructField("policy_key", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("lob_key", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("paid_amount", DecimalType(18, 2), True),
            StructField("reserve_amount", DecimalType(18, 2), True),
            StructField("recovery_amount", DecimalType(18, 2), True),
            StructField("expense_amount", DecimalType(18, 2), True),
            StructField("total_incurred", DecimalType(18, 2), True),
            StructField("is_final_payment", BooleanType(), True),
        ]
    )

    # TRAINEE NOTE — fact_policy_transaction: Policy lifecycle events
    # Grain: one row per policy lifecycle event.
    # transaction_type: "New" (first-time policy), "Renewal" (policy renewed),
    #                   "Endorsement" (mid-term change), "Cancellation" (policy cancelled)
    # premium_change: How much the premium changed due to this transaction
    #   New/Renewal → positive (full premium), Cancellation → negative (return premium)
    FACT_POLICY_TRANSACTION = StructType(
        [
            StructField("policy_txn_key", StringType(), False),
            StructField("policy_key", StringType(), True),
            StructField("date_key", IntegerType(), True),
            StructField("lob_key", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("premium_change", DecimalType(18, 2), True),
            StructField("effective_date", DateType(), True),
            StructField("processed_date", DateType(), True),
        ]
    )
