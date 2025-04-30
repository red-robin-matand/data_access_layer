import confluent_kafka
from data_access_layer.connections import KafkaConnection
from data_access_layer.connections.exceptions import DisconnectFailed


class KafkaProducerConnection(KafkaConnection):

    class KafkaConfigKeys(KafkaConnection.ConfigKeys):
        NAME = 'name'
        BROKER = 'broker'
        TOPIC = 'topic'
        CONFIG = 'config'
        CONFIG_ACKS = 'acks'
        CONFIG_RETRIES = 'retries'
        CONFIG_RETRY_BACKOFF = 'retry.backoff.ms'
        CONFIG_COMPRESSION = 'compression.type'
        PARTITIONS = 'partitions'
        SASL = 'sasl'
        SASL_USERNAME = 'sasl.username'
        SASL_PASSWORD = 'sasl.password'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if member.value not in [
                cls.CONFIG.value,
                cls.CONFIG_ACKS.value,
                cls.CONFIG_RETRIES.value,
                cls.CONFIG_RETRY_BACKOFF.value,
                cls.CONFIG_COMPRESSION.value,
                cls.SASL.value,
                cls.SASL_USERNAME.value,
                cls.SASL_PASSWORD.value,
            ]]

    def __init__(self, name: str, broker: str, topic: str, partitions: int, acks: str = 'all', retries: int = 3, retry_backoff_ms: int = 1000, compression_type: str = 'snappy',
                 sasl_username: str = None, sasl_password: str = None) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._partitions = partitions

        self._connection_engine: confluent_kafka.Producer = None
        self._config = {
            'acks': acks,
            'retries': retries,
            'retry.backoff.ms': retry_backoff_ms,
            'compression.type': compression_type,
        }
        self._sasl = all([sasl_username, sasl_password])
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.KafkaConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        producer_config = config.get(cls.KafkaConfigKeys.CONFIG.value, {})

        return cls(
            name=config[cls.KafkaConfigKeys.NAME.value],
            broker=config[cls.KafkaConfigKeys.BROKER.value],
            topic=config[cls.KafkaConfigKeys.TOPIC.value],
            partitions=config[cls.KafkaConfigKeys.PARTITIONS.value],
            acks=producer_config.get(
                cls.KafkaConfigKeys.CONFIG_ACKS.value, 'all'),
            retries=producer_config.get(
                cls.KafkaConfigKeys.CONFIG_RETRIES.value, 3),
            retry_backoff_ms=producer_config.get(
                cls.KafkaConfigKeys.CONFIG_RETRY_BACKOFF.value, 1000),
            compression_type=producer_config.get(
                cls.KafkaConfigKeys.CONFIG_COMPRESSION.value, 'snappy'),
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
            try:
                self._connection_engine.flush(timeout=self.DEFAULT_TIMEOUT_MS)
                self._connection_engine = None
            except Exception as e:
                raise DisconnectFailed(
                    f"Failed to disconnect from Kafka: {e}"
                ) from e

    def _create_engine(self) -> None:
        config = {
            'bootstrap.servers': self._broker,
            **self._config,
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
        self._connection_engine = confluent_kafka.Producer(config)
