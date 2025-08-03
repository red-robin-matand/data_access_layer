from data_access_layer.connections import Connection
import boto3
from botocore.config import Config


class CloudWatchConnection(Connection):

    class CloudWatchConfigKeys(Connection.ConfigKeys):
        NAME = 'name'
        REGION = 'region'


        @classmethod
        def required_keys(cls):
            return [member.value for member in cls]

    def __init__(self, name: str, region: str):
        super().__init__(name, )
        self._region = region


    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.CloudWatchConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            config[cls.CloudWatchConfigKeys.NAME.value],
            config[cls.CloudWatchConfigKeys.REGION.value],
        )

    def _create_engine(self) -> None:

        self._connection_engine = boto3.client(
            'cloudwatch',
            region_name=self._region,
        )

    def connect(self) -> None:
        self._create_engine()

    def disconnect(self) -> None:
        if self._connection_engine:
            self._connection_engine = None

    def check_health(self) -> None:
        try:
            self._connection_engine.describe_log_groups(limit=1)
            return True
        except:
            return False

    def create_connection_string(self) -> str:
        return f"cloudwatch://{self._region}/{self._name}"