from master_data_registry.adapters.config_resolver import env_property
import dotenv

dotenv.load_dotenv()


class MasterDataRegistryAPIConfig:

    @env_property()
    def MASTER_DATA_REGISTRY_API_USER(self, config_value: str) -> str:
        return config_value

    @env_property()
    def MASTER_DATA_REGISTRY_API_PASSWORD(self, config_value: str) -> str:
        return config_value


class MasterDataRegistryConfig(MasterDataRegistryAPIConfig):
    """
        This class aims to search for configurations in environment variables.
    """


config = MasterDataRegistryConfig()
