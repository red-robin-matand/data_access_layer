from data_access_layer.connections import DataLakeConnection
from pyiceberg.catalog.glue import GlueCatalog
import boto3

class IcebergGlueConnection(DataLakeConnection):

    class S3ConfigKeys(DataLakeConnection.ConfigKeys):
        NAME = 'name'
        CATALOG_NAME = 'catalog_name'
        WAREHOUSE = 'warehouse'
        REGION = 'region'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls]

    def __init__(self, name: str, catalog_name: str, warehouse: str, region: str) -> None:
        super().__init__(
            name=name,
        )
        self._region = region
        self._catalog_name = catalog_name
        self._warehouse = warehouse
        self._connection_engine = None

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.S3ConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            config[cls.S3ConfigKeys.NAME.value],
            config[cls.S3ConfigKeys.CATALOG_NAME.value],
            config[cls.S3ConfigKeys.WAREHOUSE.value],
            config[cls.S3ConfigKeys.REGION.value],
        )

    def _create_engine(self) -> None:

        client = boto3.client('glue', region_name=self._region)

        self._connection_engine = GlueCatalog(
            name=self._catalog_name,
            warehouse=self._warehouse,
            region=self._region,
            client=client,
        )
            

    def connect(self) -> None:
        self._create_engine()

    def disconnect(self) -> None:
        if self._connection_engine:
            self._connection_engine = None

    def check_health(self) -> None:
        try:
            self._connection_engine.list_namespaces()
            return True
        except:
            return False

    def create_connection_string(self) -> str:
        return f"glue://{self._catalog_name}"