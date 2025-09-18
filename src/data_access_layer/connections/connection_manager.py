import os

from data_access_layer.connections import CONNECTION_TYPES
from data_access_layer.connections import (
    Connection,
    ConnectionsFactory,
    ConnectionsConfigurationParser,
)


class ConnectionManager:
    _instance = None
    _connections = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(ConnectionManager, cls).__new__(cls)

            yaml_path = os.environ.get("DATA_CONNECTIONS_YAML")

            if not yaml_path:
                raise Exception(
                    "DATA_CONNECTIONS_YAML environment variable is not set")

            factory = ConnectionsFactory()
            for conn_name, conn_type in CONNECTION_TYPES.items():
                factory.register_type(
                    connection_type=conn_name, 
                    creator=conn_type,
                )

            parser = ConnectionsConfigurationParser(factory)
            connection_objs = parser.parse_connections_config(yaml_path)
            for conn in connection_objs:
                cls._connections[conn.name] = conn

        return cls._instance

    @classmethod
    def get_connection(cls, name: str) -> Connection:
        return cls._connections.get(name)

    @classmethod
    def get_connection_names(cls) -> list:
        return list(cls._connections.keys())
