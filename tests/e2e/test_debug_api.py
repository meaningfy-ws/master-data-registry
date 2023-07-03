import pandas as pd
import requests

from master_data_registry import config
from master_data_registry.services.organization_registry import get_organization_record_links


def test_debug_api(organizations_test_data, organization_test_duckdb_path):
    organization_test_duckdb_path.unlink(missing_ok=True)
    record_links = get_organization_record_links(organization_records=organizations_test_data,
                                                 unique_column_name="OrganizationAddressId",
                                                 duckdb_database_path=organization_test_duckdb_path,
                                                 )
    print(record_links)
    assert len(record_links) == len(organizations_test_data)
    organization_test_duckdb_path.unlink()


def test_debug_rest_api(organizations_test_data):
    api_url = "https://master-data-registry.meaningfy.ws"
    response = requests.post(f"{api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organizations_test_data.to_json(orient="split"),
                                 "unique_column_name": "OrganizationAddressId"
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json(), orient="split")
    assert len(links_for_records) == len(organizations_test_data)
    assert set(organizations_test_data["OrganizationAddressId"].tolist()) == set(links_for_records["unique_id_src"].tolist())
