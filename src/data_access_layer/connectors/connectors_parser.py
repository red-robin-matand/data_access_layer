import yaml

from data_access_layer.connectors import ConnectorsFactory

class ConnectorsConfigurationParser:
    def __init__(self, connectors_factory: ConnectorsFactory):
        
        self._connectors_factory = connectors_factory
        
    def parse_connectors_config(self, config_yaml_path: str) -> list:
        
        with open(config_yaml_path, 'r') as file:
            connectors_config = yaml.safe_load(file)

        connectors = []
        for connector_config in connectors_config['connectors']:
            connector_type = connector_config['type']
            connector = self._connectors_factory.create(
                connector_type=connector_type, 
                config=connector_config,
            )
            connectors.append(connector)

        return connectors
