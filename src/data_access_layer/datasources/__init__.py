from .datasource import DataSource
from .object_store_datasource import ObjectStoreDataSource
from .oltp_datasource import OLTPDataSource
from .stream_datasource import StreamDataSource
from .datalake_datasource import DataLakeDataSource

from .kafka_datasource import KafkaDataSource

from .iceberg_glue_datasource import IcebergGlueDataSource

from .s3_datasource import S3DataSource
from .postgresql_datasource import PostgreSQLDataSource
from .cloud_watch_datasource import CloudWatchDataSource
from .kafka_consumer_datasource import KafkaConsumerDataSource
from .kafka_producer_datasource import KafkaProducerDataSource
from .cloud_watch_logs_datasource import CloudWatchLogsDataSource

from data_access_layer.connections import (
    S3Connection,
    PostgreSQLConnection,
    KafkaConsumerConnection,
    KafkaProducerConnection,
    CloudWatchConnection,
    CloudWatchLogsConnection,
    IcebergGlueConnection,
)

DATA_SOURCE_TYPES = {
    S3Connection: S3DataSource,
    PostgreSQLConnection: PostgreSQLDataSource,
    KafkaConsumerConnection: KafkaConsumerDataSource,
    KafkaProducerConnection: KafkaProducerDataSource,
    CloudWatchConnection: CloudWatchDataSource,
    CloudWatchLogsConnection: CloudWatchLogsDataSource,
    IcebergGlueConnection: IcebergGlueDataSource,
}

from .datasource_manager import DataSourceManager