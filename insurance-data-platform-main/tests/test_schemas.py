"""
Unit tests for schema definitions.
Validates that all schemas are properly defined and consistent.
"""

import pytest
from pyspark.sql.types import StructType, StructField, StringType


class TestBronzeSchemas:
    """Test Bronze layer schemas."""

    def test_property_claims_csv_has_required_fields(self):
        from src.common.schemas import BronzeSchemas

        schema = BronzeSchemas.PROPERTY_CLAIMS_CSV
        assert isinstance(schema, StructType)
        field_names = [f.name for f in schema.fields]
        assert "dateOfLoss" in field_names
        assert "state" in field_names
        assert "amountPaidOnBuildingClaim" in field_names

    def test_property_claims_csv_all_string_types(self):
        """Bronze CSV schemas should have all StringType columns."""
        from src.common.schemas import BronzeSchemas

        schema = BronzeSchemas.PROPERTY_CLAIMS_CSV
        for field in schema.fields:
            assert field.dataType == StringType(), f"Field {field.name} should be StringType, got {field.dataType}"

    def test_workers_comp_csv_has_required_fields(self):
        from src.common.schemas import BronzeSchemas

        schema = BronzeSchemas.WORKERS_COMP_CSV
        field_names = [f.name for f in schema.fields]
        assert "policy_id" in field_names
        assert "class_code" in field_names
        assert "payroll_amount" in field_names

    def test_auto_claims_csv_has_required_fields(self):
        from src.common.schemas import BronzeSchemas

        schema = BronzeSchemas.AUTO_CLAIMS_CSV
        field_names = [f.name for f in schema.fields]
        assert "policy_id" in field_names
        assert "vin" in field_names
        assert "premium_amount" in field_names

    def test_umbrella_csv_has_underlying_policy_refs(self):
        from src.common.schemas import BronzeSchemas

        schema = BronzeSchemas.UMBRELLA_CSV
        field_names = [f.name for f in schema.fields]
        assert "underlying_auto_policy_id" in field_names
        assert "underlying_gl_policy_id" in field_names
        assert "underlying_wc_policy_id" in field_names


class TestSilverSchemas:
    """Test Silver layer schemas."""

    def test_property_policies_has_typed_fields(self):
        from src.common.schemas import SilverSchemas
        from pyspark.sql.types import DateType, DecimalType, BooleanType

        schema = SilverSchemas.PROPERTY_POLICIES
        field_map = {f.name: f.dataType for f in schema.fields}
        assert isinstance(field_map.get("policy_effective_date"), DateType)
        assert isinstance(field_map.get("total_building_coverage"), DecimalType)
        assert isinstance(field_map.get("primary_residence"), BooleanType)

    def test_silver_schemas_have_dq_columns(self):
        from src.common.schemas import SilverSchemas

        for schema_name in [
            "PROPERTY_POLICIES",
            "PROPERTY_CLAIMS",
            "WORKERS_COMP_POLICIES",
            "WORKERS_COMP_CLAIMS",
            "AUTO_POLICIES",
            "AUTO_CLAIMS",
            "GENERAL_LIABILITY_POLICIES",
            "GENERAL_LIABILITY_CLAIMS",
            "UMBRELLA_POLICIES",
            "UMBRELLA_CLAIMS",
        ]:
            schema = getattr(SilverSchemas, schema_name)
            field_names = [f.name for f in schema.fields]
            assert "_is_valid" in field_names, f"{schema_name} missing _is_valid"
            assert "_dq_issues" in field_names, f"{schema_name} missing _dq_issues"


class TestGoldSchemas:
    """Test Gold layer schemas."""

    def test_dim_date_has_date_key(self):
        from src.common.schemas import GoldSchemas
        from pyspark.sql.types import IntegerType

        schema = GoldSchemas.DIM_DATE
        field_map = {f.name: f for f in schema.fields}
        assert "date_key" in field_map
        assert not field_map["date_key"].nullable  # PK should be NOT NULL

    def test_dim_policy_has_foreign_keys(self):
        from src.common.schemas import GoldSchemas

        schema = GoldSchemas.DIM_POLICY
        field_names = [f.name for f in schema.fields]
        assert "lob_key" in field_names
        assert "insured_key" in field_names
        assert "agent_key" in field_names
        assert "location_key" in field_names

    def test_fact_premium_has_measures(self):
        from src.common.schemas import GoldSchemas

        schema = GoldSchemas.FACT_PREMIUM
        field_names = [f.name for f in schema.fields]
        assert "written_premium" in field_names
        assert "earned_premium" in field_names
        assert "net_premium" in field_names

    def test_fact_claim_transaction_has_key(self):
        from src.common.schemas import GoldSchemas

        schema = GoldSchemas.FACT_CLAIM_TRANSACTION
        field_map = {f.name: f for f in schema.fields}
        assert "claim_txn_key" in field_map
        assert not field_map["claim_txn_key"].nullable
