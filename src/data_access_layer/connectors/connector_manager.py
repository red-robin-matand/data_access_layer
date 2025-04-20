from data_access_layer.connectors import Connector
class ConnectorManager:
    _instance = None
    _connectors = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(ConnectorManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_connector(cls, name: str) -> Connector:
        return cls._connectors.get(name)