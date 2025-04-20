from .datasource import DataSource
from .object_store_datasource import ObjectStoreDataSource
from .oltp_datasource import OLTPDataSource
from .stream_datasource import StreamDataSource

from .kafka_datasource import KafkaDataSource

from .s3_datasource import S3DataSource
from .postgresql_datasource import PostgreSQLDataSource
from .kafka_consumer_datasource import KafkaConsumerDataSource
from .kafka_producer_datasource import KafkaProducerDataSource

from data_access_layer.connections import (
    S3Connection,
    PostgreSQLConnection,
    KafkaConsumerConnection,
    KafkaProducerConnection,
)

DATA_SOURCE_TYPES = {
    S3Connection: S3DataSource,
    PostgreSQLConnection: PostgreSQLDataSource,
    KafkaConsumerConnection: KafkaConsumerDataSource,
    KafkaProducerConnection: KafkaProducerDataSource,
}

from .datasource_manager import DataSourceManager