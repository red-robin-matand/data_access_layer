from abc import ABC, abstractmethod
from enum import Enum
from functools import cached_property

from data_access_layer.connections.exceptions.connection import MissingConfigurationKey


class Connection(ABC):

    class ConfigKeys(Enum):

        @abstractmethod
        def required_keys(self):
            pass

    def __init__(self, name: str) -> None:
        self._name = name
        self._connection_engine = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def connection_engine(self):
        return self._connection_engine

    @classmethod
    def from_dict(cls, conf_dict: dict):
        pass

    @classmethod
    def validate_dict_keys(cls, conf_dict: dict, required_keys: list) -> None:

        missing_keys = [key for key in required_keys if key not in conf_dict]
        if missing_keys:
            raise MissingConfigurationKey(
                f"Missing required keys in the configuration: {missing_keys}")

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def check_health(self) -> None:
        pass

    @abstractmethod
    @cached_property
    def create_connection_string(self) -> str:
        pass

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def __del__(self) -> None:
        self.disconnect()
