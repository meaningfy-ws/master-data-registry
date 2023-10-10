
import pathlib

TEST_DATA_PATH = pathlib.Path(__file__).parent.resolve()

ORGANIZATIONS_DUPLICATES_SAMPLE_PATH = TEST_DATA_PATH / "organizations_duplicates_sample.parquet"
TEST_ORGANIZATIONS_DUPLICATES_SAMPLE_PATH = TEST_DATA_PATH / "data_table.csv"

TEST_DEDUPLICATION_DIR_PATH = TEST_DATA_PATH / "deduplication_data"

TEST_ORGANIZATION_DEDUPLICATION_DATA_PATH = TEST_DEDUPLICATION_DIR_PATH / "organization_deduplication.csv"