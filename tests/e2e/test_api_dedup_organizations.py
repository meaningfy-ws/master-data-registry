import pandas as pd
import requests

from master_data_registry import config
from master_data_registry.resources.duckdb_databases import ORGANIZATION_DUCKDB_DATABASE_PATH


def test_api_dedup_organizations(local_api_url, organization_duplicate_records, organization_records_unique_id):
    """
    Test that the organization registry manager returns the correct links for a given record.
    """
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink(missing_ok=True)
    response = requests.post(f"{local_api_url}/api/v1/dedup_organizations",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(),
                                 "unique_column_name": organization_records_unique_id
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json())
    first_recommended_unique_ids = set(links_for_records["OrganizationRegistryID"].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records["OrganizationId"].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    response = requests.post(f"{local_api_url}/api/v1/dedup_organizations",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(),
                                 "unique_column_name": organization_records_unique_id
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json())
    second_recommended_unique_ids = set(links_for_records["OrganizationRegistryID"].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records["OrganizationId"].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    assert len(first_recommended_unique_ids) == len(second_recommended_unique_ids)
    assert first_recommended_unique_ids == second_recommended_unique_ids
    assert len(first_recommended_unique_ids) < len(organization_duplicate_records)
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink()
