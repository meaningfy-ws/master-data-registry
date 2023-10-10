import pathlib

import pytest
import pandas as pd

import splink.duckdb.comparison_library as cl
import splink.duckdb.comparison_template_library as ctl

from tests.test_data import TEST_ORGANIZATION_DEDUPLICATION_DATA_PATH


@pytest.fixture
def duplicates_records_dataframe() -> pd.DataFrame:
    return pd.DataFrame({'name': ['John Doe', 'J. Doe'] * 2,
                         'address': ['123 Main St', '47 Green St'] * 2,
                         'country': ['USA', 'Canada'] * 2,
                         })


@pytest.fixture
def reference_records_dataframe() -> pd.DataFrame:
    return pd.DataFrame({'unique_id': [1, 2],
                         'name': ['John Doe', 'J. Doe'],
                         'address': ['123 Main St', '47 Green St'],
                         'country': ['USA', 'Canada'],
                         })


@pytest.fixture
def splink_model_config() -> dict:
    return {
        "link_type": "dedupe_only",
        "comparisons": [
            cl.jaro_winkler_at_thresholds("address"),
            ctl.name_comparison("name")
        ],
        "blocking_rules_to_generate_predictions": [
            "l.country = r.country"
        ],
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": False,
    }


@pytest.fixture
def unit_test_duckdb_path() -> pathlib.Path:
    return pathlib.Path("unit_test_duckdb.db")


@pytest.fixture
def duckdb_reference_table_name() -> str:
    return "reference_table_name"


@pytest.fixture
def organization_deduplication_data() -> pd.DataFrame:
    return pd.read_csv(TEST_ORGANIZATION_DEDUPLICATION_DATA_PATH)

@pytest.fixture
def organization_data_unique_id() -> str:
    return "OrganizationAddressId"