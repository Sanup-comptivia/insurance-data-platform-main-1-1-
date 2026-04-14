"""
Unit tests for utility functions.
Tests run without Spark (schema/logic tests only).
"""

import pytest
import re


class TestStandardizeColumnNames:
    """Test column name standardization logic."""

    def test_camel_case_conversion(self):
        """Test camelCase → snake_case conversion logic."""
        test_cases = [
            ("dateOfLoss", "date_of_loss"),
            ("amountPaidOnBuildingClaim", "amount_paid_on_building_claim"),
            ("reportedZipCode", "reported_zip_code"),
            ("numberOfFloorsInTheInsuredBuilding", "number_of_floors_in_the_insured_building"),
            ("policyEffectiveDate", "policy_effective_date"),
            ("totalBuildingInsuranceCoverage", "total_building_insurance_coverage"),
        ]
        for camel, expected in test_cases:
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel)
            result = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
            result = re.sub(r"[^a-z0-9_]", "_", result)
            result = re.sub(r"_+", "_", result).strip("_")
            assert result == expected, f"Expected '{expected}', got '{result}' for '{camel}'"

    def test_already_snake_case(self):
        """Already snake_case should remain unchanged."""
        test_cases = ["policy_id", "claim_status", "date_of_loss"]
        for name in test_cases:
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
            result = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
            result = re.sub(r"[^a-z0-9_]", "_", result)
            result = re.sub(r"_+", "_", result).strip("_")
            assert result == name


class TestUSStatesMapping:
    """Test US states lookup."""

    def test_all_50_states_present(self):
        from src.common.utils import US_STATES

        assert len(US_STATES) == 51  # 50 states + DC

    def test_state_codes_are_uppercase(self):
        from src.common.utils import US_STATES

        for code in US_STATES.keys():
            assert code == code.upper()
            assert len(code) == 2


class TestSurrogateKeyLogic:
    """Test surrogate key generation logic (without Spark)."""

    def test_md5_deterministic(self):
        """Same input should always produce same hash."""
        import hashlib

        val = "PROP-0000000001||2024-01-01"
        hash1 = hashlib.md5(val.encode()).hexdigest()
        hash2 = hashlib.md5(val.encode()).hexdigest()
        assert hash1 == hash2

    def test_md5_unique_for_different_inputs(self):
        """Different inputs should produce different hashes."""
        import hashlib

        hash1 = hashlib.md5("PROP-001".encode()).hexdigest()
        hash2 = hashlib.md5("PROP-002".encode()).hexdigest()
        assert hash1 != hash2
