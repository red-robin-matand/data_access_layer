
from data_access_layer.datasources import KafkaDataSource
from data_access_layer.connections import KafkaConsumerConnection

from confluent_kafka import TopicPartition, Message

class KafkaConsumerDataSource(KafkaDataSource):

    def __init__(self, connection: KafkaConsumerConnection):
        super().__init__(connection)
        self._topic = self._connection._topic

    def consume(self, n_messages : int, timeout: int) -> list:
        messages = self._connection_engine.consume(n_messages, timeout=timeout)
        return messages
    
    def commit(self, message: dict = None, offsets: dict = None, asynchronous: bool = False) -> None:
        if message:
            self._connection_engine.commit(message, asynchronous=asynchronous)
        elif offsets:
            self._connection_engine.commit(offsets, asynchronous=asynchronous)
        else:
            raise ValueError("Either message or offsets must be provided for commit.")

    def commited(self, partitions: list) -> None:
        self._connection_engine.commited(partitions)

    def seek(self, offset : int) -> None:

        topic_partition = TopicPartition(self._topic, 0, offset)
        self._connection_engine.seek(topic_partition)

    def positions(self) -> dict:

        topic_partitions = self._connection_engine.assignment()
        return self._connection_engine.position(topic_partitions)
    
    def poll(self, timeout: int = 0) -> Message:
        message = self._connection_engine.poll(timeout=timeout)
        return message
    