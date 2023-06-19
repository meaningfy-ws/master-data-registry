from master_data_registry.services.organization_registry import get_organization_record_links


def test_organization_registry_get_organization_record_links(duplicates_records_dataframe, splink_model_config,
                                                             unit_test_duckdb_path, duckdb_reference_table_name):
    """
    Test that the registry manager returns the correct links for a given record.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    links_for_records = get_organization_record_links(organization_records=duplicates_records_dataframe,
                                                      duckdb_database_path=unit_test_duckdb_path,
                                                      linkage_model_config=splink_model_config,
                                                      )
    assert len(links_for_records) == len(duplicates_records_dataframe)
    assert set(links_for_records["unique_id_l"].tolist()) == set(duplicates_records_dataframe.index.tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    links_for_records = get_organization_record_links(organization_records=duplicates_records_dataframe,
                                                      duckdb_database_path=unit_test_duckdb_path,
                                                      linkage_model_config=splink_model_config,
                                                      )
    assert len(links_for_records) == len(duplicates_records_dataframe)
    assert set(links_for_records["unique_id_l"].tolist()) == set(duplicates_records_dataframe.index.tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    unit_test_duckdb_path.unlink()
