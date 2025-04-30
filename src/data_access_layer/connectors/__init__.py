from .connector import Connector
from .sink_connector import SinkConnector
from .source_connector import SourceConnector
from .s3_sink_connector import S3SinkConnector
from .s3_source_connector import S3SourceConnector

CONNECTOR_TYPES = {
    "s3_sink": S3SinkConnector,
    "s3_source" : S3SourceConnector,
}

from .connectors_factory import ConnectorsFactory
from .connectors_parser import ConnectorsConfigurationParser
from .connector_manager import ConnectorManager