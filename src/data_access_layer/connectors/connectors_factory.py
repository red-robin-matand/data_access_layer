from data_access_layer.connectors.exceptions import UnknownConnectorType
from data_access_layer.connectors import Connector


class ConnectorsFactory:
    
    def __init__(self):

        self._creators = {}

    def register_type(cls, connector_type: str, creator: Connector) -> None:
        
        cls._creators[connector_type] = creator

    def create(self, connection_type: str, config: dict) -> Connector:
        
        creator: Connector = self._creators.get(connection_type)
        if not creator:
            raise UnknownConnectorType(
                f"connector type {connection_type} is unknown.")

        return creator.from_dict(config)
