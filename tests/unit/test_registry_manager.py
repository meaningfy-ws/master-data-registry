from master_data_registry.adapters.registry_manager import DuckDBRegistryManager


def test_registry_manager_get_links_for_records(duplicates_records_dataframe, splink_model_config,
                                                unit_test_duckdb_path, duckdb_reference_table_name):
    """
    Test that the registry manager returns the correct links for a given record.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    registry_manager = DuckDBRegistryManager(duckdb_database_path=unit_test_duckdb_path,
                                             linkage_model_config=splink_model_config,
                                             registry_duckdb_table_name=duckdb_reference_table_name
                                             )
    links_for_records = registry_manager.get_links_for_records(data=duplicates_records_dataframe)
    assert len(links_for_records) == len(duplicates_records_dataframe)
    assert set(links_for_records["unique_id_l"].tolist()) == set(duplicates_records_dataframe.index.tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    duplicates_records_dataframe["unique_id"] = duplicates_records_dataframe["unique_id"] + 666
    links_for_records = registry_manager.get_links_for_records(data=duplicates_records_dataframe)
    assert len(links_for_records) == len(duplicates_records_dataframe)
    assert set(links_for_records["unique_id_l"].tolist()) == set(duplicates_records_dataframe.index.tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    assert all(links_for_records["match_probability"] < 1.00)
    unit_test_duckdb_path.unlink()
