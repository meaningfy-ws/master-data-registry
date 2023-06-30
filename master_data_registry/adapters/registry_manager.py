import pandas as pd
import pathlib

from master_data_registry.adapters.duckdb_adapter import DuckDBAdapter
from master_data_registry.adapters.linkage_engine import SplinkRecordLinkageEngine
from master_data_registry.adapters.registry_manager_abc import RegistryManagerABC


def merge_cluster_to_single_representation(cluster_df: pd.DataFrame) -> dict:
    cluster_column_names = cluster_df.columns
    result_dict = {}
    for column_name in cluster_column_names:
        candidate_column_value = cluster_df[column_name].value_counts().sort_values(ascending=False).head(
            1).index.to_list()
        if len(candidate_column_value) == 1:
            result_dict[column_name] = candidate_column_value[0]
        else:
            result_dict[column_name] = None
    return result_dict


CLUSTER_ID_COLUMN_NAME = "cluster_id"
UNIQUE_ID_COLUMN_NAME = "unique_id"
UNIQUE_ID_L_COLUMN_NAME = "unique_id_l"
UNIQUE_ID_R_COLUMN_NAME = "unique_id_r"
MATCH_PROBABILITY_COLUMN_NAME = "match_probability"


class DuckDBRegistryManager(RegistryManagerABC):
    """DuckDB registry manager."""

    def __init__(self, duckdb_database_path: pathlib.Path,
                 linkage_model_config: dict,
                 registry_duckdb_table_name: str
                 ):
        self.duckdb_database_path = duckdb_database_path
        self.duckdb_adapter = DuckDBAdapter(database_path=self.duckdb_database_path)
        self.linker_engine = SplinkRecordLinkageEngine(model_config=linkage_model_config,
                                                       duckdb_adapter=self.duckdb_adapter)
        self.registry_table_name = registry_duckdb_table_name

    def __minimize_cluster_records(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Minimizes clusters.
        :param data:
        :return:
        """
        prepare_data = data.copy()
        prepare_data.drop(columns=[UNIQUE_ID_COLUMN_NAME], inplace=True)
        clusters = prepare_data.groupby(CLUSTER_ID_COLUMN_NAME)
        clusters_data = []
        for cluster_id, cluster in clusters:
            result_dict = {"cluster_id": cluster_id}
            if len(cluster) == 1:
                result_dict.update(cluster.to_dict(orient="records")[0])
            else:
                cluster_column_names = cluster.columns
                for column_name in cluster_column_names:
                    candidate_column_value = cluster[column_name].value_counts().index
                    if len(candidate_column_value) > 0:
                        result_dict[column_name] = candidate_column_value[0]
                    else:
                        result_dict[column_name] = None
            clusters_data.append(result_dict)
        minimized_clusters = pd.DataFrame(clusters_data)
        minimized_clusters.rename(columns={"cluster_id": UNIQUE_ID_COLUMN_NAME}, inplace=True)
        return minimized_clusters

    def __get_pairs_from_unlinked_data(self, unlinked_data: pd.DataFrame) -> pd.DataFrame:
        """
        Gets pairs from unlinked data.
        :param unlinked_data:
        :return:
        """
        result_unlinked_data_pairs = unlinked_data[[UNIQUE_ID_COLUMN_NAME]].copy()
        result_unlinked_data_pairs.rename(columns={UNIQUE_ID_COLUMN_NAME: UNIQUE_ID_L_COLUMN_NAME}, inplace=True)
        result_unlinked_data_pairs[UNIQUE_ID_R_COLUMN_NAME] = result_unlinked_data_pairs[UNIQUE_ID_L_COLUMN_NAME]
        result_unlinked_data_pairs[MATCH_PROBABILITY_COLUMN_NAME] = 1.0
        return result_unlinked_data_pairs

    def get_links_for_records(self, data: pd.DataFrame, unique_column_name: str = None,
                              threshold_match_probability: float = 0.8) -> pd.DataFrame:
        """
        Links a dataframe of records.
        :param data:
        :param unique_column_name:
        :param threshold_match_probability:
        :return:
        """
        prepared_data = self.linker_engine.preprocess_data(data=data, unique_column_name=unique_column_name)
        deduplicated_data_clusters = self.linker_engine.dedupe_records_and_clustering(data=prepared_data,
                                                                                      threshold_match_probability=threshold_match_probability)

        minimized_clusters = self.__minimize_cluster_records(data=deduplicated_data_clusters)
        print(minimized_clusters)
        if self.duckdb_adapter.check_if_table_exists(table_name=self.registry_table_name):
            linked_clusters = self.linker_engine.link_records(data=minimized_clusters,
                                                              reference_table_name=self.registry_table_name,
                                                              threshold_match_probability=threshold_match_probability)
            print(linked_clusters)
            linked_unique_ids = linked_clusters[UNIQUE_ID_L_COLUMN_NAME].unique().tolist()
            unlinked_clusters = minimized_clusters[~minimized_clusters[UNIQUE_ID_COLUMN_NAME].isin(linked_unique_ids)]
            result_linked_data_pairs = linked_clusters[[UNIQUE_ID_L_COLUMN_NAME, UNIQUE_ID_R_COLUMN_NAME,
                                                        MATCH_PROBABILITY_COLUMN_NAME]].copy()
            print(result_linked_data_pairs)
            if len(unlinked_clusters) > 0:
                self.duckdb_adapter.insert_dataframe(table_name=self.registry_table_name, data=unlinked_clusters)
                result_unlinked_data_pairs = self.__get_pairs_from_unlinked_data(unlinked_data=unlinked_clusters)
                result_linked_data_pairs = pd.concat([result_linked_data_pairs, result_unlinked_data_pairs])
        else:
            self.duckdb_adapter.create_table(table_name=self.registry_table_name, data=minimized_clusters)
            result_linked_data_pairs = self.__get_pairs_from_unlinked_data(unlinked_data=minimized_clusters)

        result_linked_data_pairs.rename(columns={UNIQUE_ID_L_COLUMN_NAME: CLUSTER_ID_COLUMN_NAME}, inplace=True)
        result_linked_data_pairs = result_linked_data_pairs.merge(right=deduplicated_data_clusters,
                                                                  on=CLUSTER_ID_COLUMN_NAME, how="right")
        result_linked_data_pairs.rename(columns={UNIQUE_ID_COLUMN_NAME: UNIQUE_ID_L_COLUMN_NAME}, inplace=True)
        return result_linked_data_pairs[[UNIQUE_ID_L_COLUMN_NAME,
                                         UNIQUE_ID_R_COLUMN_NAME,
                                         MATCH_PROBABILITY_COLUMN_NAME]].copy()
