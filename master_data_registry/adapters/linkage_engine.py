import pandas as pd
from splink.duckdb.linker import DuckDBLinker
from splink.splink_dataframe import SplinkDataFrame

from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter
from master_data_registry.adapters.linkage_engine_abc import RecordLinkageEngineABC

DEFAULT_SRC_TABLE_NAME = "src_table"
UNIQUE_ID_COLUMN_NAME = "unique_id"
BLOCKING_RULES_TO_GENERATE_PREDICTIONS_KEY = "blocking_rules_to_generate_predictions"
MAX_RANDOM_SAMPLING_PAIRS = 100000
DEFAULT_SAMPLING_SEED = 6


class SplinkRecordLinkageEngine(RecordLinkageEngineABC):
    """
    Record linkage engine based on Splink.
    """

    def __init__(self, model_config: dict, duckdb_adapter: DuckDBAdapter):
        """
        Initializes a Splink record linkage engine.
        :param model_config: Splink configuration.
        """
        self.model_config = model_config
        self.duckdb_adapter = duckdb_adapter

    def preprocess_data(self, data: pd.DataFrame, unique_column_name: str = None) -> pd.DataFrame:
        """
        Preprocesses data before linking or deduplication.
        :param data: Dataframe to preprocess.
        :param unique_column_name: Name of the column that uniquely identifies each record.
        :return: Preprocessed dataframe.
        """
        if UNIQUE_ID_COLUMN_NAME not in data.columns:
            data[UNIQUE_ID_COLUMN_NAME] = data[unique_column_name] if unique_column_name else data.index
        return data

    def finetune_model_config(self, data: pd.DataFrame, max_random_sampling_pairs: int,
                              sampling_seed: int = DEFAULT_SAMPLING_SEED) -> dict:
        """
        Fine-tunes the model configuration based on the data.
        :param data: Dataframe to preprocess.
        :param max_random_sampling_pairs: Maximum number of random sampling pairs to use for finetuning.
        :param sampling_seed: Seed for random sampling.
        :return: Finetuned model configuration.
        """
        linker = DuckDBLinker(input_table_or_tables=[data],
                              settings_dict=self.model_config,
                              connection=self.duckdb_adapter.get_connection()
                              )
        linker.estimate_u_using_random_sampling(max_pairs=max_random_sampling_pairs, seed=sampling_seed)
        if BLOCKING_RULES_TO_GENERATE_PREDICTIONS_KEY in self.model_config:
            blocking_rules = self.model_config[BLOCKING_RULES_TO_GENERATE_PREDICTIONS_KEY]
            for blocking_rule in blocking_rules:
                linker.estimate_parameters_using_expectation_maximisation(blocking_rule=blocking_rule)

        self.model_config = linker.save_model_to_json()
        return self.model_config

    def dedupe_records(self, data: pd.DataFrame, threshold_match_probability: float = 0.8) -> pd.DataFrame:
        """
        Deduplicate a dataframe of records.
        :param data: Dataframe to deduplicate.
        :param threshold_match_probability: Minimum match probability to consider two records as duplicates.
        :return: Dataframe of deduplicated records.
        """
        linkage_engine_settings = self.model_config.copy()
        linkage_engine_settings["link_type"] = "dedupe_only"
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        self.duckdb_adapter.create_table(data=data, table_name=DEFAULT_SRC_TABLE_NAME)
        linker = DuckDBLinker(input_table_or_tables=DEFAULT_SRC_TABLE_NAME,
                              settings_dict=linkage_engine_settings,
                              connection=self.duckdb_adapter.get_connection()
                              )
        result_df = linker.predict(threshold_match_probability=threshold_match_probability).as_pandas_dataframe()
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        return result_df

    def link_records(self, data: pd.DataFrame, reference_table_name: str,
                     threshold_match_probability: float = 0.8) -> pd.DataFrame:
        """
        :param data: Dataframe to link.
        :param reference_table_name: Name of the reference duckdb table to link against.
        :param threshold_match_probability: Minimum match probability to consider two records as duplicates.
        :return: Dataframe of linked records.
        """
        linkage_engine_settings = self.model_config.copy()
        linkage_engine_settings["link_type"] = "link_only"
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        self.duckdb_adapter.create_table(data=data, table_name=DEFAULT_SRC_TABLE_NAME)
        linker = DuckDBLinker(input_table_or_tables=[DEFAULT_SRC_TABLE_NAME, reference_table_name],
                              input_table_aliases=["__ori", "_dest"],
                              connection=self.duckdb_adapter.get_connection(),
                              settings_dict=linkage_engine_settings)
        result_df = linker.predict(threshold_match_probability=threshold_match_probability).as_pandas_dataframe()
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        return result_df

    def dedupe_records_and_clustering(self, data: pd.DataFrame,
                                      threshold_match_probability: float = 0.8) -> pd.DataFrame:
        """
        Deduplicate a dataframe of records and cluster the results.
        :param data: Dataframe to deduplicate.
        :param threshold_match_probability: Minimum match probability to consider two records as duplicates.
        :return: Dataframe of deduplicated records grouped into clusters.
        """
        linkage_engine_settings = self.model_config.copy()
        linkage_engine_settings["link_type"] = "dedupe_only"
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        self.duckdb_adapter.create_table(data=data, table_name=DEFAULT_SRC_TABLE_NAME)
        linker = DuckDBLinker(input_table_or_tables=DEFAULT_SRC_TABLE_NAME,
                              connection=self.duckdb_adapter.get_connection(),
                              settings_dict=linkage_engine_settings)
        dedup_data = linker.predict(threshold_match_probability=threshold_match_probability)
        result_clusters = linker.cluster_pairwise_predictions_at_threshold(dedup_data,
                                                                           threshold_match_probability=threshold_match_probability)
        result_df = result_clusters.as_pandas_dataframe()
        self.duckdb_adapter.delete_table(DEFAULT_SRC_TABLE_NAME)
        return result_df
