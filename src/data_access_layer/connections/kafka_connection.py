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
