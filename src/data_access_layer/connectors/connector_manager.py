import os

from data_access_layer.connectors import CONNECTOR_TYPES
from data_access_layer.connectors import (
    Connector,
    ConnectorsFactory,
    ConnectorsConfigurationParser,
)


class ConnectorManager:
    _instance = None
    _connectors = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(ConnectorManager, cls).__new__(cls)

            yaml_path = os.environ.get("CONNECTIONS_YAML")

            if not yaml_path:
                raise Exception(
                    "CONNECTIONS_YAML environment variable is not set")

            factory = ConnectorsFactory()
            for conn_name, conn_type in CONNECTOR_TYPES.items():
                factory.register_type(
                    connection_type=conn_name, 
                    creator=conn_type,
                )

            parser = ConnectorsConfigurationParser(factory)
            connector_objs = parser.parse_connectors_config(yaml_path)
            for conn in connector_objs:
                cls._connectors[conn.name] = conn

        return cls._instance

    @classmethod
    def get_connector(cls, name: str) -> Connector:
        return cls._connectors.get(name)

    @classmethod
    def get_connector_names(cls) -> list:
        return list(cls._connectors.keys())
    