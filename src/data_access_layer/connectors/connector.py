from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property

from data_access_layer.datasources import (
    DataSource,
    KafkaConsumerDataSource,
    DataSourceManager
)

class Connector(ABC):

    def __init__(self, name: str, kafka_consumer_datasource_name : str, data_sink_name : str) -> None:
        self._name = name
        self._kafka_consumer_datasource_name = kafka_consumer_datasource_name
        self._data_sink_name = data_sink_name

        self._data_source_manager : DataSourceManager = None
        self._kafka_consumer_datasource : KafkaConsumerDataSource = None
        self._data_sink : DataSource = None

    @property
    def name(self) -> str:
        return self._name

    def connect(self) -> None:
        self._data_source_manager = DataSourceManager()
        self._kafka_consumer_datasource = self._data_source_manager.get_data_source(self._kafka_consumer_datasource_name)
        self._data_sink = self._data_source_manager.get_data_source(self._data_sink_name)

    def disconnect(self) -> None:
        self._data_sink.disconnect()
        self._kafka_consumer_datasource.disconnect()

    def check_health(self) -> None:
        self._data_sink.check_health()
        self._kafka_consumer_datasource.check_health()

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def __del__(self) -> None:
        self.disconnect()

    @abstractmethod
    def consume_from_broker_to_sink(self, sink_args : dict) -> None:
        pass
