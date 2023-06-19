from master_data_registry.services.organization_registry import get_organization_record_links


def test_organization_registry_get_organization_record_links(organization_duplicate_records,
                                                             organization_records_unique_id,
                                                             organization_test_duckdb_path):
    """
    Test that the organization registry manager returns the correct links for a given record.
    """
    organization_test_duckdb_path.unlink(missing_ok=True)
    links_for_records = get_organization_record_links(organization_records=organization_duplicate_records,
                                                      unique_column_name=organization_records_unique_id,
                                                      duckdb_database_path=organization_test_duckdb_path,
                                                      )
    first_recommended_unique_ids = set(links_for_records["unique_id_r"].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records["unique_id_l"].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] == 1.0)
    links_for_records = get_organization_record_links(organization_records=organization_duplicate_records,
                                                      unique_column_name=organization_records_unique_id,
                                                      duckdb_database_path=organization_test_duckdb_path,
                                                      )
    second_recommended_unique_ids = set(links_for_records["unique_id_r"].unique().tolist())
    assert len(links_for_records) == len(organization_duplicate_records)
    assert set(links_for_records["unique_id_l"].tolist()) == set(
        organization_duplicate_records[organization_records_unique_id].tolist())
    assert all(links_for_records["match_probability"] >= 0.99)
    assert len(first_recommended_unique_ids) == len(second_recommended_unique_ids)
    assert first_recommended_unique_ids == second_recommended_unique_ids
    assert len(first_recommended_unique_ids) < len(organization_duplicate_records)
    organization_test_duckdb_path.unlink()

