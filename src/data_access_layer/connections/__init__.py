from .connection import Connection
from .oltp_connection import OLTPConnection
from .stream_connection import StreamConnection
from .object_store_connection import ObjectStoreConnection

from .kafka_connection import KafkaConnection

from .s3_connection import S3Connection
from .postgresql_connection import PostgreSQLConnection

from .kafka_consumer_connection import KafkaConsumerConnection
from .kafka_producer_connection import KafkaProducerConnection

CONNECTION_TYPES = {
    "s3": S3Connection,
    "kafka_consumer": KafkaConsumerConnection,
    "kafka_producer": KafkaProducerConnection,
    "postgresql": PostgreSQLConnection,
}

from .connections_factory import ConnectionsFactory
from .connections_parser import ConnectionsConfigurationParser
from .connection_manager import ConnectionManager