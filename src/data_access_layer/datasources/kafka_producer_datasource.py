
from data_access_layer.datasources import KafkaDataSource
from data_access_layer.connections import KafkaProducerConnection
from data_access_layer.datasources.exceptions import MessageDeliveryError

import logging

logger = logging.getLogger(__name__)


class KafkaProducerDataSource(KafkaDataSource):

    def __init__(self, connection: KafkaProducerConnection):
        super().__init__(connection)

        self._connection : KafkaProducerConnection

        self._partitions = self._connection._partitions

    def delivery_report(self, err, msg) -> None:
        if err is not None:
            message = f"Message delivery failed: {err}"
            logger.error(message)
            raise MessageDeliveryError(message)
        else:
            message = f"Message delivered to {msg.topic()} [{msg.partition()}]"
            logger.info(message)

    def produce(self, key: str, value: str, partition: int = None) -> None:
        self._connection_engine.produce(
            topic=self._topic,
            key=key,
            value=value,
            partition=partition,
            on_delivery=self.delivery_report
        )

    def batch_produce(self, messages: list, back_pressure_threshold: int = None) -> None:
        for message in messages:
            self._connection_engine.produce(
                topic=self._topic,
                key=message["key"],
                value=message["value"],
                partition=message["partition"],
            )

            if back_pressure_threshold and self.buffer_length() > back_pressure_threshold:
                self.flush()

        self.flush()

    def poll(self, timeout: int = 0) -> None:
        self._connection_engine.poll(timeout=timeout)

    def flush(self) -> None:
        self._connection_engine.flush(timeout=self.DEFAULT_TIMEOUT_MS)

    def buffer_length(self) -> int:
        return len(self._connection_engine)

    def is_buffering(self) -> bool:
        is_buffering = self.buffer_length() > 0
        return is_buffering
