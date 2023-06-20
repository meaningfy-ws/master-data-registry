import pathlib

from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter


def remove_reference_table(reference_table_name: str, duckdb_database_path: pathlib.Path) -> bool:
    """
    Removes the reference table for organizations.
    :param reference_table_name: The name of the reference table.
    :param duckdb_database_path: The path to the DuckDB database.
    :return: True if the table was removed, False otherwise.
    """
    duckdb_adapter = DuckDBAdapter(database_path=duckdb_database_path)
    duckdb_adapter.delete_table(table_name=reference_table_name)
    return not duckdb_adapter.check_if_table_exists(table_name=reference_table_name)
