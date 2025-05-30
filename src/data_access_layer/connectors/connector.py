from abc import ABC, abstractmethod
from enum import Enum

from data_access_layer.connectors.exceptions.connector import MissingConfigurationKey
from data_access_layer.datasources import (
    DataSource,
    DataSourceManager
)


class Connector(ABC):

    class ConfigKeys(Enum):
        NAME = 'name'
        SOURCE_NAME = 'source_name'
        SINK_NAME = 'sink_name'

        @classmethod
        def required_keys(cls):
            [member.value for member in cls]

    def __init__(self, name: str, source_name: str, sink_name: str) -> None:
        self._name = name
        self._source_name = source_name
        self._sink_name = sink_name

        self._data_source_manager: DataSourceManager = None
        self._source: DataSource = None
        self._sink: DataSource = None

    @property
    def name(self) -> str:
        return self._name

    @classmethod
    def validate_dict_keys(cls, conf_dict: dict, required_keys: list) -> None:

        missing_keys = [key for key in required_keys if key not in conf_dict]
        if missing_keys:
            raise MissingConfigurationKey(
                f"Missing required keys in the configuration: {missing_keys}")

    @classmethod
    def from_dict(cls, conf_dict: dict):
        config_keys = cls.ConfigKeys.required_keys()
        cls.validate_dict_keys(conf_dict, config_keys)

        return cls(
            conf_dict[cls.ConfigKeys.NAME.value],
            conf_dict[cls.ConfigKeys.SOURCE_NAME.value],
            conf_dict[cls.ConfigKeys.SINK_NAME.value],
        )

    def connect(self) -> None:
        self._data_source_manager = DataSourceManager()
        self._source = self._data_source_manager.get_data_source(
            self._source_name)
        self._sink = self._data_source_manager.get_data_source(self._sink_name)

    def disconnect(self) -> None:
        self._sink.disconnect()
        self._source.disconnect()

    def check_health(self) -> None:
        self._sink.check_health()
        self._source.check_health()

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def __del__(self) -> None:
        self.disconnect()

    