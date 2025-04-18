from .datasource import DataSource
from .object_store_datasource import ObjectStoreDataSource
from .oltp_datasource import OLTPDataSource

from .s3_datasource import S3DataSource

from data_access_layer.connections.s3_connection import S3Connection



DATA_SOURCE_TYPES = {
    S3Connection: S3DataSource,
}
