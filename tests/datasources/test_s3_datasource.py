import pytest
from unittest.mock import Mock, patch
from data_access_layer.datasources import S3DataSource
from data_access_layer.connections import S3Connection


class TestS3DataSource:
    @pytest.fixture
    def mock_connection(self):
        connection = Mock(spec=S3Connection)

        connection._bucket = 'test-bucket'
        connection._region = 'eu-west-1'
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
    def mock_s3_filesystem(self):
        with patch("pyarrow.fs.S3FileSystem") as mock_fs:
            yield mock_fs

    @pytest.fixture
    def datasource(self, mock_connection, mock_s3_filesystem):
        mock_fs_instance = Mock()
        mock_s3_filesystem.return_value = mock_fs_instance

        datasource = S3DataSource(mock_connection)
        datasource.connect()
        return datasource

    @patch("os.path.getsize", return_value=1024)
    @patch("builtins.open", new_callable=Mock)
    def test_upload_file_success(self, mock_open, mock_getsize, datasource, mock_connection):
        local_path = "test_file.txt"
        s3_key = "folder/test_file.txt"

        datasource.upload_file(local_path, s3_key)

        mock_connection._connection_engine.upload_file.assert_called_once_with(
            local_path,
            mock_connection._bucket,
            s3_key
        )

    # def test_download_file_success(self, datasource, mock_connection):
    #     local_path = "test_file.txt"
    #     s3_key = "folder/test_file.txt"
    #     bucket = "test-bucket"

    #     with patch("builtins.open", Mock()):
    #         datasource.download_file( s3_key, local_path)

    #     mock_connection._connection_engine.download_file.assert_called_once_with(
    #         bucket, s3_key, local_path
    #     )

    def test_delete_object(self, datasource, mock_connection):

        bucket = "test-bucket"
        s3_key = "folder/test_file.txt"

        datasource.delete_file(s3_key)

        mock_connection._connection_engine.delete_object.assert_called_once_with(
            Bucket=bucket, Key=s3_key
        )
