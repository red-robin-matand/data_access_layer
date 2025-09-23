from data_access_layer.connections import DataLakeConnection
from pyiceberg.catalog import load_catalog

class IcebergGlueConnection(DataLakeConnection):

    class S3ConfigKeys(DataLakeConnection.ConfigKeys):
        NAME = 'name'
        CATALOG_NAME = 'catalog_name'
        CATALOG_TYPE = 'catalog_type'
        WAREHOUSE = 'warehouse'
        REGION = 'region'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls]

    def __init__(self, name: str, catalog_name: str, catalog_type: str, warehouse: str, region: str) -> None:
        super().__init__(
            name=name,
        )
        self._region = region
        self._catalog_name = catalog_name
        self._catalog_type = catalog_type
        self._warehouse = warehouse
        self._connection_engine = None

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.S3ConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            config[cls.S3ConfigKeys.NAME.value],
            config[cls.S3ConfigKeys.REGION.value],
            config[cls.S3ConfigKeys.CATALOG_NAME.value],
            config[cls.S3ConfigKeys.CATALOG_TYPE.value],
            config[cls.S3ConfigKeys.WAREHOUSE.value],
        )

    def _create_engine(self) -> None:
        
        self._connection_engine = load_catalog(
            name=self._catalog_name,
            catalog_type=self._catalog_type,
            warehouse=self._warehouse,
            region=self._region,
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
        return f"{self._catalog_type}://{self._catalog_name}"