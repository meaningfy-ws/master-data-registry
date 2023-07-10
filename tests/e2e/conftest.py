import pathlib

import pandas as pd
import pytest

from tests.test_data import ORGANIZATIONS_DUPLICATES_SAMPLE_PATH, TEST_ORGANIZATIONS_DUPLICATES_SAMPLE_PATH


@pytest.fixture
def organization_duplicate_records() -> pd.DataFrame:
    return pd.read_parquet(ORGANIZATIONS_DUPLICATES_SAMPLE_PATH)


@pytest.fixture
def organization_records_unique_id() -> str:
    return "OrganizationId"


@pytest.fixture
def organization_test_duckdb_path() -> pathlib.Path:
    return pathlib.Path("organization_test_duckdb.db")


@pytest.fixture
def local_api_url() -> str:
    return "http://localhost:8000"


@pytest.fixture
def organizations_test_data() -> pd.DataFrame:
    return pd.read_csv(TEST_ORGANIZATIONS_DUPLICATES_SAMPLE_PATH)
