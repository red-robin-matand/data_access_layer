from data_access_layer.connections import ConnectionManager

from data_access_layer.datasources import DataSource
from data_access_layer.datasources import DATA_SOURCE_TYPES


class DataSourceManager:
    _instance = None
    _data_sources = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(DataSourceManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_connection_names(cls) -> list:
        return ConnectionManager().get_connection_names()

    def get_data_source(self, connection_name: str) -> DataSource:
        
        if connection_name not in self._data_sources:
            
            connection = ConnectionManager().get_connection(connection_name)
            if not connection:
                existing_connections = self.get_connection_names()
                raise ValueError(
                    f"No connection found for name: {connection_name}, existing connections: {existing_connections}")

            data_source_cls = DATA_SOURCE_TYPES.get(type(connection))
            
            if not data_source_cls:
                raise ValueError(
                    f"No data source registered for connection type: {type(connection)}")

            data_source = data_source_cls(connection)
            data_source.connect()
            self._data_sources[connection_name] = data_source

        return self._data_sources[connection_name]

    def shutdown(self) -> None:
        for _, data_source in self._data_sources.items():
            data_source.disconnect()

    def __del__(self) -> None:
        self.shutdown()
