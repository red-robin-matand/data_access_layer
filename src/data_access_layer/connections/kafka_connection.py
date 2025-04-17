import confluent_kafka
from abc import ABC, abstractmethod
from data_access_layer.connections import StreamConnection


class KafkaConnection(StreamConnection, ABC):

    DEFAULT_TIMEOUT_MS = 10000

    def __init__(self, name: str, broker: str, topic: str) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._connection_engine: confluent_kafka.Client = None

    class KafkaConfigKeys(StreamConnection.ConfigKeys):
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

    def check_health(self) -> bool:
        if not self._connection_engine:
            return False
        try:
            self._connection_engine.list_topics(
                timeout=self.DEFAULT_TIMEOUT_MS)
            return True
        except Exception as e:
            return False

    @property
    def create_connection_string(self) -> str:
        return f"kafka://{self._broker}/{self._topic}"
