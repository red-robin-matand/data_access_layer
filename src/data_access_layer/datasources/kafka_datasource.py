from abc import ABC

from data_access_layer.datasources import StreamDataSource
from data_access_layer.connections import KafkaConnection


class KafkaDataSource(StreamDataSource, ABC):

    def __init__(self, connection: KafkaConnection):
        super().__init__(connection)

        self._connection : KafkaConnection
        self._default_timeout_ms = self._connection.DEFAULT_TIMEOUT_MS

    def get_broker_metadata(self) -> dict:

        metadata = self._connection_engine.list_topics(
            timeout=self._default_timeout_ms)

        return metadata
