import confluent_kafka
from data_access_layer.connections import KafkaConnection


class KafkaProducerConnection(KafkaConnection):
    def __init__(self, name: str, broker: str, topic: str) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._connection_engine: confluent_kafka.Producer = None

    class KafkaConfigKeys(KafkaConnection.ConfigKeys):
        NAME = 'name'
        BROKER = 'broker'
        TOPIC = 'topic'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls]

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.KafkaConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        return cls(
            name=config[cls.KafkaConfigKeys.NAME.value],
            broker=config[cls.KafkaConfigKeys.BROKER.value],
            topic=config[cls.KafkaConfigKeys.TOPIC.value],
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
                raise e

    def _create_engine(self) -> None:
        config = {
            'bootstrap.servers': self._broker,
        }
        self._connection_engine = confluent_kafka.Producer(config)
