import confluent_kafka
from data_access_layer.connections import KafkaConnection
from data_access_layer.connections.exceptions import ConnectionFailed


class KafkaConsumerConnection(KafkaConnection):

    class KafkaConfigKeys(KafkaConnection.ConfigKeys):
        NAME = 'name'
        BROKER = 'broker'
        TOPIC = 'topic'
        GROUP_ID = 'group_id'
        OFFSET = 'offset'
        SASL = 'sasl'
        SASL_USERNAME = 'sasl.username'
        SASL_PASSWORD = 'sasl.password'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if member.value not in [
                cls.SASL.value,
                cls.SASL_USERNAME.value,
                cls.SASL_PASSWORD.value,
            ]]
        
    def __init__(self, name: str, broker: str, topic: str, group_id: str, offset: str,
                 sasl_username : str = None, sasl_password : str = None) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._group_id = group_id
        self._offset = offset
        self._connection_engine: confluent_kafka.Consumer = None
        self._sasl = all([sasl_username, sasl_password])
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.KafkaConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            name=config[cls.KafkaConfigKeys.NAME.value],
            broker=config[cls.KafkaConfigKeys.BROKER.value],
            topic=config[cls.KafkaConfigKeys.TOPIC.value],
            group_id=config[cls.KafkaConfigKeys.GROUP_ID.value],
            offset=config[cls.KafkaConfigKeys.OFFSET.value],
            sasl_username=config.get(
                cls.KafkaConfigKeys.SASL_USERNAME.value, None),
            sasl_password=config.get(
                cls.KafkaConfigKeys.SASL_PASSWORD.value, None),
        )

    def connect(self):
        if not self._connection_engine:
            self._create_engine()

    def disconnect(self) -> None:
        if self._connection_engine:
            self._connection_engine.close()
            self._connection_engine = None

    def _create_engine(self) -> None:
        config = {
            'bootstrap.servers': self._broker,
            'group.id': self._group_id,
            'auto.offset.reset': self._offset,
        }
        sasl_args = {}
        if self._sasl:
            sasl_args = {
                'sasl.username': self._sasl_username,
                'sasl.password': self._sasl_password,
                'sasl.mechanisms': 'PLAIN',
                'security.protocol': 'SASL_SSL',
            }
            config.update(sasl_args)
        self._connection_engine = confluent_kafka.Consumer(config)

        try:
            self._connection_engine.subscribe([self._topic])
        except Exception as e:
            message = f"Failed to subscribe to topic {self._topic}: {e}"
            raise ConnectionFailed(message) from e

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def offset(self) -> str:
        return self._offset
