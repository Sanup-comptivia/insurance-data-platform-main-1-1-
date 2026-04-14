"""
Data Quality framework for the Insurance Data Platform.
Provides reusable DQ checks for Silver layer transformations.

TRAINEE GUIDE — Why does data quality matter in insurance?
Insurance companies make pricing, reserving, and claims decisions based on
data. A single bad record (e.g. a negative premium, a future date of loss)
can distort financial reports or trigger incorrect payouts.

This module provides a fluent (chainable) API to declare checks on a
DataFrame, then apply them all at once, producing two output columns:
  _is_valid  : True if ALL checks passed for that row
  _dq_issues : Semicolon-separated list of which checks failed (if any)

The "flag, don't drop" philosophy:
  Bad rows are KEPT in the Silver layer (they are just flagged). This lets
  data engineers investigate root causes. Gold layers then filter to only
  _is_valid = True rows for clean analytics.

Usage pattern (fluent/chainable API):
  dq = DataQualityChecker(df)
  df_checked = (
      dq.check_not_null("policy_id")
        .check_positive("premium_amount")
        .check_date_range("effective_date", "2000-01-01", "2030-12-31")
        .check_in_set("lob_code", ["PROP", "AUTO", "WC", "GL", "UMB"])
        .apply()
  )
  # df_checked now has _is_valid and _dq_issues columns
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityChecker:
    """Configurable data quality checker for insurance data.

    TRAINEE NOTE — Design Pattern: Builder / Fluent Interface
    Each check_*() method adds one check to self.checks and then
    returns `self`. Returning `self` is what makes it "chainable" — you
    can call multiple methods in sequence without intermediate variables:
      dq.check_not_null("a").check_positive("b").apply()

    This pattern is called a "fluent interface" or "builder pattern".
    It reads like English, making the checks easy to understand at a glance.

    Internal state:
      self.df     : The DataFrame being validated (may be mutated by some checks)
      self.checks : List of (check_name, fail_condition) tuples accumulated
                    as you call check_*() methods. Applied all at once in apply().
    """

    def __init__(self, df: DataFrame):
        self.df = df
        self.checks = []  # Each entry: (check_name: str, condition: Column)

    def check_not_null(self, column: str, check_name: str = None):
        """Add a not-null check.

        TRAINEE NOTE:
        Null values in key columns (like policy_id or claim_id) are critical
        failures — they make a row unidentifiable and unrelatable to other tables.
        This check flags any row where the specified column is null.

        F.col(column).isNull() returns True when the value is null → check fails.
        """
        name = check_name or f"null_{column}"
        self.checks.append((name, F.col(column).isNull()))
        return self  # Return self to allow method chaining

    def check_positive(self, column: str, check_name: str = None):
        """Add a check that a numeric column is positive.

        TRAINEE NOTE:
        Financial amounts (premiums, coverage limits, claim payments) should
        never be negative in insurance data. A negative premium or claim amount
        almost certainly indicates a data entry error or a source system issue.

        Note: We check for < 0 (strictly negative), not <= 0.
        Zero is allowed because some records legitimately have $0 amounts
        (e.g. a waived deductible or a $0 claim on a denied claim).
        Null values pass this check (isNull() does not evaluate to < 0).
        """
        name = check_name or f"non_positive_{column}"
        self.checks.append((name, F.col(column) < 0))
        return self

    def check_date_range(
        self, column: str, min_date: str = "1900-01-01", max_date: str = "2100-12-31", check_name: str = None
    ):
        """Add a date range validation.

        TRAINEE NOTE:
        Insurance policies have valid date ranges. An effective_date of 1850
        or 2999 is almost certainly a data error. Common checks:
          - effective_date: must be within a realistic policy year range
          - date_of_loss: cannot be in the future (can't file a claim for
            something that hasn't happened yet)

        The check fails if the date is either BEFORE min_date OR AFTER max_date.
        Null dates pass this check (we use check_not_null separately for that).
        """
        name = check_name or f"date_range_{column}"
        self.checks.append((name, (F.col(column) < F.lit(min_date)) | (F.col(column) > F.lit(max_date))))
        return self

    def check_in_set(self, column: str, valid_values: list, check_name: str = None):
        """Add a check that values are in an allowed set.

        TRAINEE NOTE:
        Categorical columns should only contain known/expected values.
        Examples:
          lob_code must be in ["PROP", "AUTO", "WC", "GL", "UMB"]
          claim_status must be in ["Open", "Closed", "Pending"]

        The condition fails when:
          - The value IS NOT in the valid list (invalid category), AND
          - The value IS NOT null (nulls are allowed; use check_not_null separately)

        Why exclude nulls? Because null means "unknown" — it is handled separately
        by check_not_null(). This way you can independently decide whether null
        is allowed AND whether known values are in the valid set.
        """
        name = check_name or f"invalid_{column}"
        self.checks.append((name, ~F.col(column).isin(valid_values) & F.col(column).isNotNull()))
        return self

    def check_regex(self, column: str, pattern: str, check_name: str = None):
        """Add a regex pattern validation.

        TRAINEE NOTE:
        Some columns have strict format requirements:
          policy_id : must match "PROP-\\d{10}" (e.g. "PROP-0000000001")
          zip_code  : must be 5 digits ("\\d{5}")
          phone     : must match a specific format

        F.col(column).rlike(pattern) uses Java regex syntax.
        The check fails when:
          - The value does NOT match the pattern, AND
          - The value IS NOT null (nulls handled separately)
        """
        name = check_name or f"pattern_{column}"
        self.checks.append((name, ~F.col(column).rlike(pattern) & F.col(column).isNotNull()))
        return self

    def check_unique(self, columns: list, check_name: str = None):
        """Flag duplicate records based on key columns.

        TRAINEE NOTE:
        This check uses a Window function (COUNT over a partition) to detect
        duplicates. Rows where the count is > 1 are flagged as duplicates.

        Important: This check MUTATES self.df by adding a helper column "_dup_check".
        The helper column is cleaned up in apply().

        When to use this check vs. deduplicate():
          - check_unique() FLAGS duplicates so you can see them in the output.
          - deduplicate() in utils.py REMOVES duplicates, keeping only one row.
          Use check_unique in Silver to report the problem; use deduplicate
          to resolve it before writing the clean Silver table.

        Example: If policy_id "PROP-001" appears 3 times,
          all 3 rows get _dup_check=3 and are flagged with this check name.
        """
        from pyspark.sql.window import Window

        name = check_name or f"duplicate_{'_'.join(columns)}"
        # COUNT(*) over the partition: how many times does this key appear?
        window = Window.partitionBy(*columns).orderBy(F.lit(1))
        self.df = self.df.withColumn("_dup_check", F.count("*").over(window))
        self.checks.append((name, F.col("_dup_check") > 1))
        return self

    def check_referential_integrity(
        self, column: str, reference_df: DataFrame, ref_column: str, check_name: str = None
    ):
        """Check that all values in column exist in reference DataFrame.

        TRAINEE NOTE:
        Referential integrity ensures foreign key relationships are valid.
        Example: Every claim's policy_id should exist in the policies table.
        A claim with a policy_id that doesn't exist is an "orphan record".

        How it works:
          1. Take distinct values from reference_df[ref_column].
          2. Left-join the main DataFrame on the FK column.
          3. After a left join, rows that DIDN'T match get null in the
             joined columns. We add a _ref_exists=True column so that
             non-matching rows have _ref_exists=null.
          4. The check fails when _ref_exists IS null (i.e. no match found).

        The "_ref_exists" helper column is cleaned up in apply().

        This check is more expensive than others because it requires a join.
        Use it selectively on critical FK columns.
        """
        name = check_name or f"orphan_{column}"
        ref_values = reference_df.select(ref_column).distinct()
        self.df = self.df.join(
            ref_values.withColumn("_ref_exists", F.lit(True)), self.df[column] == ref_values[ref_column], "left"
        ).drop(ref_column)
        self.checks.append((name, F.col("_ref_exists").isNull()))
        return self

    def apply(self) -> DataFrame:
        """Apply all configured checks and return DataFrame with DQ columns.

        TRAINEE NOTE:
        This is the terminal method that actually runs the checks.
        It delegates to add_dq_columns() in utils.py which:
          1. Builds an array of failed check names for each row
          2. Derives _is_valid (True if no failures)
          3. Derives _dq_issues (semicolon-joined failure names)

        After calling apply(), the returned DataFrame has all original columns
        PLUS _is_valid and _dq_issues. The original data is never modified;
        only these two columns are added.

        Helper columns added by check_unique() and check_referential_integrity()
        are cleaned up here to keep the output tidy.
        """
        from src.common.utils import add_dq_columns

        result = add_dq_columns(self.df, self.checks)
        # Clean up any helper columns left by check_unique / check_referential_integrity
        for col_name in result.columns:
            if col_name.startswith("_dup_check") or col_name == "_ref_exists":
                result = result.drop(col_name)
        return result

    def get_summary(self) -> DataFrame:
        """Get a summary of DQ issues after applying checks.

        TRAINEE NOTE:
        This convenience method applies all checks and then counts how many
        rows are valid vs. invalid. Useful for quick monitoring output:
          total_records | valid_records | invalid_records | validity_pct
          1,000,000     | 995,000       | 5,000           | 99.5

        A validity_pct below a threshold (e.g. 95%) would trigger an alert
        in a production pipeline to prevent silently loading bad data.

        Note: This method calls apply() internally, so it triggers the full
        DQ computation. Don't call get_summary() and then apply() separately
        — that would compute DQ twice. Use one or the other.
        """
        result = self.apply()
        total = result.count()
        valid = result.filter(F.col("_is_valid")).count()
        invalid = total - valid

        spark = result.sparkSession
        summary_data = [(total, valid, invalid, round(valid / total * 100, 2) if total > 0 else 0.0)]
        return spark.createDataFrame(
            summary_data, ["total_records", "valid_records", "invalid_records", "validity_pct"]
        )
