import confluent_kafka
from data_access_layer.connections import KafkaConnection

class KafkaConsumerConnection(KafkaConnection):
    def __init__(self, name: str, broker: str, topic: str, group_id: str, offset: str) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._group_id = group_id
        self._offset = offset
        self._connection_engine: confluent_kafka.Consumer = None

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
        self._connection_engine = confluent_kafka.Consumer(config)
        
        try:
            self._connection_engine.subscribe([self._topic])
        except Exception as e:
            raise

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def offset(self) -> str:
        return self._offset
