import json
import pathlib

import pandas as pd

from master_data_registry.adapters.registry_manager import DuckDBRegistryManager
from master_data_registry.resources.duckdb_databases import ORGANIZATION_DUCKDB_DATABASE_PATH
from master_data_registry.resources.splink_model_settings import ORGANIZATION_SPLINK_MODEL_V1_PATH

ORGANIZATIONS_DUCKDB_REFERENCE_TABLE_NAME = "organizations_reference_table"


def get_organization_record_links(organization_records: pd.DataFrame,
                                  unique_column_name: str = None,
                                  threshold_match_probability: float = 0.8,
                                  duckdb_database_path: pathlib.Path = ORGANIZATION_DUCKDB_DATABASE_PATH,
                                  linkage_model_config: dict = None,
                                  registry_duckdb_table_name: str = None
                                  ) -> pd.DataFrame:
    """
    Links a dataframe of organization records.
    :param organization_records:
    :param unique_column_name:
    :param threshold_match_probability:
    :param duckdb_database_path:
    :param linkage_model_config:
    :param registry_duckdb_table_name:
    :return:
    """
    if registry_duckdb_table_name is None:
        registry_duckdb_table_name = ORGANIZATIONS_DUCKDB_REFERENCE_TABLE_NAME

    if linkage_model_config is None:
        linkage_model_config = json.loads(ORGANIZATION_SPLINK_MODEL_V1_PATH.read_text(encoding="utf-8"))

    registry_manager = DuckDBRegistryManager(duckdb_database_path=duckdb_database_path,
                                             linkage_model_config=linkage_model_config,
                                             registry_duckdb_table_name=registry_duckdb_table_name
                                             )
    links_for_records = registry_manager.get_links_for_records(data=organization_records,
                                                               unique_column_name=unique_column_name,
                                                               threshold_match_probability=threshold_match_probability)
    return links_for_records

