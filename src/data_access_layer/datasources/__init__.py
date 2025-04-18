from .datasource import DataSource
from .object_store_datasource import ObjectStoreDataSource
from .oltp_datasource import OLTPDataSource

from .s3_datasource import S3DataSource
from .postgresql_datasource import PostgreSQLDataSource

from data_access_layer.connections import S3Connection
from data_access_layer.connections import PostgreSQLConnection


DATA_SOURCE_TYPES = {
    S3Connection: S3DataSource,
    PostgreSQLConnection: PostgreSQLDataSource,
}
