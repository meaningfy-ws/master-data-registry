import json

import pandas as pd
import requests

from master_data_registry import config
from master_data_registry.entrypoints.api.main import DATAFRAME_TO_JSON_ORIENT_TYPE
from master_data_registry.resources.duckdb_databases import ORGANIZATION_DUCKDB_DATABASE_PATH
from master_data_registry.resources.splink_model_settings import ORGANIZATION_SPLINK_MODEL_V1_PATH

UNIQUE_ID_SRC_COLUMN_NAME = "unique_id_src"
UNIQUE_ID_DST_COLUMN_NAME = "unique_id_dst"


def test_api_dedup_organizations(local_api_url, organization_duplicate_records, organization_records_unique_id):
    """
    Test that the organization registry manager returns the correct links for a given record.
    """
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink(missing_ok=True)
    response = requests.post(f"{local_api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE),
                                 "unique_column_name": organization_records_unique_id
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    reference_column_id = UNIQUE_ID_DST_COLUMN_NAME
    links_for_records = pd.read_json(response.json(), orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    first_recommended_unique_ids = set(links_for_records[reference_column_id].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records[UNIQUE_ID_SRC_COLUMN_NAME].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    response = requests.post(f"{local_api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE),
                                 "unique_column_name": organization_records_unique_id
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json(), orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    second_recommended_unique_ids = set(links_for_records[reference_column_id].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records[UNIQUE_ID_SRC_COLUMN_NAME].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    assert len(first_recommended_unique_ids) == len(second_recommended_unique_ids)
    assert first_recommended_unique_ids == second_recommended_unique_ids
    assert len(first_recommended_unique_ids) < len(organization_duplicate_records)
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink()


def test_api_dedup_organizations_with_reference_table_name(local_api_url, organization_duplicate_records,
                                                           organization_records_unique_id):
    """
    Test that the organization registry manager returns the correct links for a given record.
    """
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink(missing_ok=True)
    response = requests.post(f"{local_api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE),
                                 "unique_column_name": organization_records_unique_id,
                                 "reference_table_name": "tmp_table_name",
                                 "linkage_model_config": json.loads(
                                     ORGANIZATION_SPLINK_MODEL_V1_PATH.read_text(encoding="utf-8"))
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json(), orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    reference_column_id = UNIQUE_ID_DST_COLUMN_NAME
    first_recommended_unique_ids = set(links_for_records[reference_column_id].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records[UNIQUE_ID_SRC_COLUMN_NAME].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    response = requests.post(f"{local_api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE),
                                 "unique_column_name": organization_records_unique_id
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    links_for_records = pd.read_json(response.json(), orient=DATAFRAME_TO_JSON_ORIENT_TYPE)
    second_recommended_unique_ids = set(links_for_records[reference_column_id].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records[UNIQUE_ID_SRC_COLUMN_NAME].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    assert len(first_recommended_unique_ids) == len(second_recommended_unique_ids)
    assert first_recommended_unique_ids == second_recommended_unique_ids
    assert len(first_recommended_unique_ids) < len(organization_duplicate_records)
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink()


def test_api_dedup_organizations_and_delete_reference_table(local_api_url, organization_duplicate_records,
                                                            organization_records_unique_id):
    """
    Test that the organization registry manager returns the correct links for a given record and deletes the reference table.
    """
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink(missing_ok=True)
    response = requests.post(f"{local_api_url}/api/v1/dedup_and_link",
                             json={
                                 "dataframe_json": organization_duplicate_records.to_json(orient=DATAFRAME_TO_JSON_ORIENT_TYPE),
                                 "unique_column_name": organization_records_unique_id,
                                 "reference_table_name": "tmp_table_name",
                                 "linkage_model_config": json.loads(
                                     ORGANIZATION_SPLINK_MODEL_V1_PATH.read_text(encoding="utf-8"))
                             },
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    response = requests.post(f"{local_api_url}/api/v1/reference_tables/remove",
                             json={"reference_table_name": "tmp_table_name"},
                             auth=(config.MASTER_DATA_REGISTRY_API_USER, config.MASTER_DATA_REGISTRY_API_PASSWORD)
                             )
    assert response.status_code == 200
    assert response.json()["remove_result"]
    ORGANIZATION_DUCKDB_DATABASE_PATH.unlink()


def test_api_health(local_api_url):
    """
    Test that the health endpoint returns the correct status.
    """
    response = requests.get(f"{local_api_url}/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "OK"
