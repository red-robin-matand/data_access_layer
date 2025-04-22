from .connector import Connector
from .sink_connector import SinkConnector
from .s3_sink_connector import S3SinkConnector

CONNECTOR_TYPES = {
    "s3_sink": S3SinkConnector,
}

from .connectors_factory import ConnectorsFactory
from .connectors_parser import ConnectorsConfigurationParser
from .connector_manager import ConnectorManager