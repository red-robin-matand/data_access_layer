import pytest
from unittest.mock import Mock, patch, ANY
from data_access_layer.connections import S3Connection
from data_access_layer.connections.exceptions.connection import MissingConfigurationKey
from botocore.exceptions import ClientError

import re


class TestS3Connection:
    @pytest.fixture
    def connection_config(self):
        return {
            "name": "test_s3",
            "type": "s3",
            "access_key": "test_key",
            "secret_key": "test_secret",
            "region": "us-east-1",
            "bucket": "test-bucket",
            "connections": 5
        }

    def test_connection_creation(self, connection_config):
        connection = S3Connection.from_dict(connection_config)
        assert connection.name == "test_s3"
        assert connection._bucket == "test-bucket"

    @patch('boto3.client')
    def test_connection_success(self, mock_boto3_client, connection_config):
        mock_s3 = Mock()
        mock_boto3_client.return_value = mock_s3

        connection = S3Connection.from_dict(connection_config)
        connection.connect()

        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id='test_key',
            aws_secret_access_key='test_secret',
            region_name='us-east-1',
            config=ANY
        )

    @patch('boto3.client')
    def test_connection_failure(self, mock_boto3_client, connection_config):
        mock_boto3_client.side_effect = ClientError(
            error_response={
                'Error': {'Code': 'InvalidAccessKeyId', 'Message': 'Invalid access key'}},
            operation_name='CreateClient'
        )

        connection = S3Connection.from_dict(connection_config)
        with pytest.raises(ClientError):
            connection.connect()

    def test_disconnect(self, connection_config):
        connection = S3Connection.from_dict(connection_config)
        mock_s3 = Mock()
        connection._connection_engine = mock_s3

        connection.disconnect()
        assert connection._connection_engine is None

    @patch('boto3.client')
    def test_connection_health_check(self, mock_boto3, connection_config):
        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3
        mock_s3.list_buckets.return_value = {'Buckets': []}

        connection = S3Connection.from_dict(connection_config)
        connection.connect()

        assert connection.check_health() is True

    def test_s3_connection_missing_required(self):
        incomplete_config = {
            "name": "test_s3",
            "type": "s3",
            "region": "us-east-1",
            # Missing required fields: bucket, access_key, secret_key, connections
        }

        with pytest.raises(MissingConfigurationKey, match=re.escape("Missing required keys in the configuration: ['access_key', 'secret_key', 'connections', 'bucket']")):
            S3Connection.from_dict(incomplete_config)

    def test_s3_connection_valid_config(self):
        valid_config = {
            "name": "test_s3",
            "type": "s3",
            "bucket": "test-bucket",
            "region": "us-east-1",
            "access_key": "test_key",
            "secret_key": "test_secret",
            "connections": 4,
        }

        connection = S3Connection.from_dict(valid_config)
        assert connection.name == "test_s3"
        assert connection._bucket == "test-bucket"
        assert connection._region == "us-east-1"
        assert connection._access_key == "test_key"
        assert connection._secret_key == "test_secret"
        assert connection._connections == 4
