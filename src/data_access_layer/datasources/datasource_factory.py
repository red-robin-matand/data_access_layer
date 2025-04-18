from data_access_layer.connections import Connection
from data_access_layer.datasources import DataSource


class DataSourceFactory:
    _registered_data_sources = {}

    @classmethod
    def register_data_source(cls, connection_type: str, data_source_cls : DataSource) -> None:
        cls._registered_data_sources[connection_type] = data_source_cls

    @classmethod
    def create_data_source(cls, connection_type: str, connection : Connection) -> DataSource:
        data_source_cls = cls._registered_data_sources.get(connection_type)
        
        if not data_source_cls:
            raise ValueError(f"No data source registered for connection type: {connection_type}")
        
        return data_source_cls(connection)
