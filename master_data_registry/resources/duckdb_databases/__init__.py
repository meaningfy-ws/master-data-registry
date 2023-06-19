import pathlib

DUCKDB_DATABASES_PATH = pathlib.Path(__file__).parent.resolve()

ORGANIZATION_DUCKDB_DATABASE_PATH = DUCKDB_DATABASES_PATH / "organizations_registry_duckdb.db"
