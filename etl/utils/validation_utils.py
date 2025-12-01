"""
Utility helpers to run Great Expectations validations in a simple way.
"""

import os
import pandas as pd
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
GE_ROOT_DIR = os.path.join(PROJECT_ROOT, "expectations")


def get_ge_context() -> ge.DataContext:
    """Load Great Expectations context from expectations/ folder."""
    return ge.DataContext(context_root_dir=GE_ROOT_DIR)


def run_ge_validation(
    df: pd.DataFrame,
    suite_name: str,
    batch_identifier: str = "default_batch",
    fail_on_error: bool = True,
) -> bool:
    """Run Great Expectations validation on a Pandas DataFrame."""

    context = get_ge_context()

    batch_request = RuntimeBatchRequest(
        datasource_name="default_pandas_datasource",
        data_connector_name="default_runtime_data_connector",
        data_asset_name="in_memory_dataframe",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": batch_identifier},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Run validation
    results = validator.validate()

    if results.success:
        print(f"[GE] Validation PASSED for suite: {suite_name}")
    else:
        print(f"[GE] Validation FAILED for suite: {suite_name}")
        context.build_data_docs()
        print("[GE] Data Docs built under expectations/data_docs/")

        if fail_on_error:
            raise ValueError(f"Validation failed for expectation suite: {suite_name}")

    return results.success
