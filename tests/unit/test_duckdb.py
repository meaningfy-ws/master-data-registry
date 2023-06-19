import pandas as pd

from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter


def test_duckdb_database_creation(unit_test_duckdb_path):
    unit_test_duckdb_path.unlink(missing_ok=True)
    assert not unit_test_duckdb_path.exists()
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert unit_test_duckdb_path.exists()
    unit_test_duckdb_path.unlink()


def test_duckdb_get_list_of_tables_names(unit_test_duckdb_path):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert duckdb_adapter.get_list_of_tables_names() == []
    unit_test_duckdb_path.unlink()


def test_duckdb_check_if_table_exists(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    unit_test_duckdb_path.unlink()


def test_duckdb_create_table(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    unit_test_duckdb_path.unlink()


def test_duckdb_read_dataframe(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    assert duckdb_adapter.read_dataframe(table_name="organizations").equals(duplicates_records_dataframe)
    unit_test_duckdb_path.unlink()


def test_duckdb_insert_dataframe(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.insert_dataframe(data=duplicates_records_dataframe, table_name="organizations")
    new_df = pd.concat([duplicates_records_dataframe, duplicates_records_dataframe],
                       ignore_index=True)
    duckdb_adapter.read_dataframe(table_name="organizations").equals(new_df)
    unit_test_duckdb_path.unlink()


def test_duckdb_delete_table(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.delete_table(table_name="organizations")
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    unit_test_duckdb_path.unlink()


def test_duckdb_get_connection(unit_test_duckdb_path, duplicates_records_dataframe):
    unit_test_duckdb_path.unlink(missing_ok=True)
    duckdb_adapter = DuckDBAdapter(database_path=unit_test_duckdb_path)
    assert not duckdb_adapter.check_if_table_exists(table_name="organizations")
    duckdb_adapter.create_table(data=duplicates_records_dataframe, table_name="organizations")
    assert duckdb_adapter.check_if_table_exists(table_name="organizations")
    assert duckdb_adapter.get_connection()
    unit_test_duckdb_path.unlink()
