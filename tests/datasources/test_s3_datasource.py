import pytest
from unittest.mock import Mock, patch
import boto3
from botocore.exceptions import ClientError
from data_access_layer.datasources import S3DataSource
from data_access_layer.connections import S3Connection

class TestS3DataSource:
    @pytest.fixture
    def mock_connection(self):
        connection = Mock(spec=S3Connection)
        
        connection._bucket = 'test-bucket'
        connection.name = 'test-s3'
        
        mock_engine = Mock()
        mock_engine.upload_file = Mock()
        mock_engine.head_object = Mock()
        mock_engine.delete_object = Mock()
        mock_engine.list_objects = Mock()
        mock_engine.download_file = Mock()
        
        connection._connection_engine = mock_engine
        type(connection).connection_engine = property(lambda x: mock_engine)
        
        return connection

    @pytest.fixture
    def datasource(self, mock_connection):
        datasource = S3DataSource(mock_connection)
        datasource._connection_engine = mock_connection.connection_engine
        return datasource

    def test_upload_file_success(self, datasource, mock_connection):
        local_path = "test_file.txt"
        s3_key = "folder/test_file.txt"

        with patch("builtins.open", Mock()):
            datasource.upload_file(local_path, s3_key)

        mock_connection._connection_engine.upload_file.assert_called_once_with(
            local_path,
            mock_connection._bucket,
            s3_key
        )

    def test_download_file_success(self, datasource, mock_connection):
        local_path = "test_file.txt"
        s3_key = "folder/test_file.txt"
        bucket = "test-bucket"

        with patch("builtins.open", Mock()):
            datasource.download_file( s3_key, local_path)

        mock_connection._connection_engine.download_file.assert_called_once_with(
            bucket, s3_key, local_path
        )

    def test_delete_object(self, datasource, mock_connection):

        bucket = "test-bucket"
        s3_key = "folder/test_file.txt"

        datasource.delete_file(s3_key)

        mock_connection._connection_engine.delete_object.assert_called_once_with(
            Bucket=bucket, Key=s3_key
        )