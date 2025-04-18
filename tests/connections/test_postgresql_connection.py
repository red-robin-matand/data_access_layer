import pytest
from unittest.mock import Mock, patch
from sqlalchemy.exc import OperationalError
from data_access_layer.connections import PostgreSQLConnection
from data_access_layer.connections.exceptions.connection import MissingConfigurationKey
from botocore.exceptions import ClientError

import re


class TestPostgreSQLConnection:
    @pytest.fixture
    def connection_config(self):
        return {
            "name": "test_postgres",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass"
        }

    def test_connection_creation(self, connection_config):
        connection = PostgreSQLConnection.from_dict(connection_config)
        assert connection.name == "test_postgres"
        assert connection._database == "test_db"
        assert connection._host == "localhost"

    @patch('data_access_layer.connections.postgresql_connection.create_engine')
    def test_connection_success(self, mock_create_engine, connection_config):
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        connection = PostgreSQLConnection.from_dict(connection_config)
        connection.connect()

        mock_create_engine.assert_called_once()

    @patch('data_access_layer.connections.postgresql_connection.create_engine')
    def test_connection_failure(self, mock_create_engine, connection_config):
        mock_create_engine.side_effect = OperationalError(
            "statement", "params", "orig")

        connection = PostgreSQLConnection.from_dict(connection_config)
        with pytest.raises(OperationalError):
            connection.connect()

    def test_disconnect(self, connection_config):
        connection = PostgreSQLConnection.from_dict(connection_config)
        mock_engine = Mock()
        connection._connection_engine = mock_engine

        connection.disconnect()
        mock_engine.dispose.assert_called_once()

    @patch('data_access_layer.connections.postgresql_connection.create_engine')
    def test_connection_health_check(self, mock_create_engine, connection_config):
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        # Mock the execute method that would be called during health check
        mock_engine.execute = Mock()

        connection = PostgreSQLConnection.from_dict(connection_config)
        connection.connect()

        assert connection.check_health() is True

    def test_postgresql_connection_missing_required(self):
        incomplete_config = {
            "name": "test_postgres",
            "type": "postgresql",
            "host": "localhost",
            # Missing required fields: port, database, username, password
        }

        with pytest.raises(MissingConfigurationKey):
            PostgreSQLConnection.from_dict(incomplete_config)

    def test_postgresql_connection_valid_config(self):
        valid_config = {
            "name": "test_postgres",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass"
        }

        connection = PostgreSQLConnection.from_dict(valid_config)
        assert connection.name == "test_postgres"
        assert connection._host == "localhost"
        assert connection._port == 5432
        assert connection._database == "test_db"
        assert connection._username == "test_user"
        assert connection._password == "test_pass"
