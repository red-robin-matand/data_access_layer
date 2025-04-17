from data_access_layer.connections.exceptions.connection import UnknownConnectionType
from data_access_layer.connections import Connection


class ConnectionsFactory:

    def __init__(self):

        self._creators = {}

    def register_type(self, connection_type: str, creator: Connection) -> None:
        
        self._creators[connection_type] = creator

    def create(self, connection_type: str, config: dict) -> Connection:
        
        creator: Connection = self._creators.get(connection_type)
        if not creator:
            raise UnknownConnectionType(
                f"Connection type {connection_type} is unknown.")

        return creator.from_dict(config)
