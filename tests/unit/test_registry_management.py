from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter
from master_data_registry.services.registry_management import remove_reference_table


def test_remove_reference_table(duplicates_records_dataframe, unit_test_duckdb_path, duckdb_reference_table_name):
    """
    Tests the remove_reference_table function.
    """
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name=duckdb_reference_table_name)
    assert duckdb_adapter.check_if_table_exists(table_name=duckdb_reference_table_name)
    remove_result = remove_reference_table(reference_table_name=duckdb_reference_table_name,
                                           duckdb_database_path=unit_test_duckdb_path)
    assert remove_result
    assert not duckdb_adapter.check_if_table_exists(table_name=duckdb_reference_table_name)
    unit_test_duckdb_path.unlink()
