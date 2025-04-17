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

    def connect(self):
        if not self._connection_engine:
            self._create_engine()
            
    def disconnect(self) -> None:
        if self._connection_engine:
            try:
                self._connection_engine.flush(timeout=self.DEFAULT_TIMEOUT_MS)
                super().disconnect()
            except Exception as e:
                raise e

    def _create_engine(self) -> None:
        config = {
            'bootstrap.servers': self._broker,
        }
        self._connection_engine = confluent_kafka.Producer(config)
