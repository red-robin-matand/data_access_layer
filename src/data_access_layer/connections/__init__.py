from .connection import Connection
from .oltp_connection import OLTPConnection
from .stream_connection import StreamConnection
from .object_store_connection import ObjectStoreConnection
from .data_lake_connection import DataLakeConnection

from .kafka_connection import KafkaConnection

from .iceberg_glue_connection import IcebergGlueConnection

from .s3_connection import S3Connection
from .postgresql_connection import PostgreSQLConnection
from .cloud_watch_connection import CloudWatchConnection
from .cloud_watch_logs_connection import CloudWatchLogsConnection

from .kafka_consumer_connection import KafkaConsumerConnection
from .kafka_producer_connection import KafkaProducerConnection

CONNECTION_TYPES = {
    "s3": S3Connection,
    "kafka_consumer": KafkaConsumerConnection,
    "kafka_producer": KafkaProducerConnection,
    "postgresql": PostgreSQLConnection,
    "cloudwatch": CloudWatchConnection,
    "cloudwatch_logs": CloudWatchLogsConnection,
    "iceberg_glue": IcebergGlueConnection,
}

from .connections_factory import ConnectionsFactory
from .connections_parser import ConnectionsConfigurationParser
from .connection_manager import ConnectionManager