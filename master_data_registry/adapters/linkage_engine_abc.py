import abc
import pandas as pd


class RecordLinkageEngineABC(abc.ABC):
    """
    Abstract base class for record linkage engines.
    """

    @abc.abstractmethod
    def dedupe_records(self, data: pd.DataFrame, threshold_match_probability: float) -> pd.DataFrame:
        """
        Deduplicates a dataframe of records.
        """
        pass

    @abc.abstractmethod
    def link_records(self, data: pd.DataFrame, reference_table_name: str,
                     threshold_match_probability: float) -> pd.DataFrame:
        """
        Links a dataframe of records.
        """
        pass

    @abc.abstractmethod
    def dedupe_records_and_clustering(self, data: pd.DataFrame,
                                      threshold_match_probability: float) -> pd.DataFrame:
        """
        Deduplicate a dataframe of records and clusters the results.
        """
        pass