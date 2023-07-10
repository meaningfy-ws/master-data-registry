import pathlib

import duckdb
import pandas as pd


class DuckDBAdapter:
    """
    Adapter for DuckDB.
    """

    def __init__(self, database_path: pathlib.Path):
        """
        Initializes a DuckDBAdapter.
        :param database_path:
        :return:
        """
        self.database_path = database_path
        self.connection = duckdb.connect(database=str(database_path), read_only=False)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return self.connection

    def get_list_of_tables_names(self) -> list:
        """
        Gets a list of table names from a DuckDB database.
        :return:
        """
        return self.connection.execute("SHOW ALL TABLES;").df()["name"].to_list()

    def check_if_table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in a DuckDB database.
        :param table_name:
        :return: True if the table exists, False otherwise.
        """
        return table_name in self.get_list_of_tables_names()

    def create_table(self, data: pd.DataFrame, table_name: str):
        """
        Writes a dataframe to a DuckDB table.
        :param data: The dataframe to write.
        :param table_name: The name of the table to write the dataframe to.
        :return:
        """
        self.connection.register("tmp_data", data)
        self.connection.execute(f"CREATE TABLE {table_name} AS SELECT * FROM tmp_data;")
        self.connection.unregister("tmp_data")


    def read_dataframe(self, table_name: str) -> pd.DataFrame:
        """
        Reads a dataframe from a DuckDB table.
        :param table_name: The name of the table to read the dataframe from.
        :return: The dataframe.
        """
        return self.connection.sql(f"SELECT * FROM {table_name}").df()

    def insert_dataframe(self, data: pd.DataFrame, table_name: str):
        """
        Inserts a dataframe into a DuckDB table.
        :param data: The dataframe to insert.
        :param table_name: The name of the table to insert the dataframe into.
        :return:
        """
        self.connection.register("data", data)
        self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM data")
        self.connection.unregister("data")

    def delete_table(self, table_name: str, if_exists: bool = True):
        """
        Deletes a table from a DuckDB database.
        :param table_name: The name of the table to delete.
        :param if_exists: If True, the table will only be deleted if it exists.
        :return:
        """
        if if_exists:
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name};")
        else:
            self.connection.execute(f"DROP TABLE {table_name};")

    def __del__(self):
        """
        Closes the connection to the DuckDB database.
        :return:
        """
        self.connection.close()
