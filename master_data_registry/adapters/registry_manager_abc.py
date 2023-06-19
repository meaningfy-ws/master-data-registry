import abc
import pandas as pd


class RegistryManagerABC(abc.ABC):

    @abc.abstractmethod
    def get_links_for_records(self, data: pd.DataFrame, unique_column_name: str,
                              threshold_match_probability: float) -> pd.DataFrame:
        """
        Links a dataframe of records.
        """
        pass
