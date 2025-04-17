import yaml

from data_access_layer.connections import ConnectionsFactory


class ConnectionsConfigurationParser:

    def __init__(self, connection_factory: ConnectionsFactory):
        
        self._connections_factory = connection_factory

    def parse_connections_config(self, connections_yaml_file_path: str) -> list:
        
        with open(connections_yaml_file_path, 'r') as file:
            connections_config = yaml.safe_load(file)

        connections = []
        for connection_config in connections_config['connections']:
            connection_type = connection_config['type']
            connection = self._connections_factory.create(
                connection_type=connection_type, 
                config=connection_config
            )
            connections.append(connection)

        return connections
