from data_access_layer.connections import ObjectStoreConnection
import boto3
from botocore.config import Config


class S3Connection(ObjectStoreConnection):

    class S3ConfigKeys(ObjectStoreConnection.ConfigKeys):
        NAME = 'name'
        REGION = 'region'
        CONNECTIONS = 'connections'
        BUCKET = 'bucket'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls]

    def __init__(self, name: str, region: str, connections: int, bucket: str):
        super().__init__(name, )
        self._region = region
        self._connections = connections
        self._bucket = bucket

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.S3ConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            config[cls.S3ConfigKeys.NAME.value],
            config[cls.S3ConfigKeys.REGION.value],
            config[cls.S3ConfigKeys.CONNECTIONS.value],
            config[cls.S3ConfigKeys.BUCKET.value],
        )

    def _create_engine(self) -> None:
        config = Config(max_pool_connections=self._connections)

        self._connection_engine = boto3.client(
            's3',
            region_name=self._region,
            config=config,
        )

    def connect(self) -> None:
        self._create_engine()

    def disconnect(self) -> None:
        if self._connection_engine:
            self._connection_engine = None

    def check_health(self) -> None:
        try:
            self._connection_engine.list_buckets()
            return True
        except:
            return False

    def create_connection_string(self) -> str:
        return f"s3://{self._bucket}"