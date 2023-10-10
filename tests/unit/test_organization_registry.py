from master_data_registry.services.organization_registry import get_organization_record_links
import hashlib


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


def test_organization_deduplication_data(organization_deduplication_data, organization_data_unique_id, unit_test_duckdb_path):
    unit_test_duckdb_path.unlink(missing_ok=True)

    organization_deduplication_data[organization_data_unique_id] = organization_deduplication_data[
        organization_data_unique_id].apply(lambda x: hashlib.sha256(x.encode()).hexdigest()[:10])
    organization_deduplication_data = organization_deduplication_data.head(10)
    links_for_records = get_organization_record_links(organization_records=organization_deduplication_data,
                                                 unique_column_name=organization_data_unique_id,
                                                 duckdb_database_path=unit_test_duckdb_path
                                                 )
    assert len(links_for_records) == len(organization_deduplication_data)
    assert set(links_for_records["unique_id_l"].tolist()) == set(organization_deduplication_data[organization_data_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    organization_deduplication_data[organization_data_unique_id] = organization_deduplication_data[
        organization_data_unique_id].apply(lambda x: hashlib.sha256(x.encode()).hexdigest()[:10])
    links_for_records = get_organization_record_links(organization_records=organization_deduplication_data,
                                                 unique_column_name=organization_data_unique_id,
                                                 duckdb_database_path=unit_test_duckdb_path
                                                 )
    assert len(links_for_records) == len(organization_deduplication_data)
    assert set(links_for_records["unique_id_l"].tolist()) == set(organization_deduplication_data[organization_data_unique_id].tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    unit_test_duckdb_path.unlink()
