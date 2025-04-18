from abc import ABC

from data_access_layer.connections import Connection


class DataSource(ABC):

    def __init__(self, connection: Connection) -> None:

        self._connection = connection
        self._connection_engine = None

    def connect(self) -> None:
        self._connection.connect()
        self._connection_engine = self._connection.connection_engine

    def disconnect(self) -> None:
        self._connection.disconnect()

    def check_health(self) -> None:
        return self._connection.check_health()

    def __enter__(self) -> None:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def __del__(self) -> None:
        self.disconnect()
