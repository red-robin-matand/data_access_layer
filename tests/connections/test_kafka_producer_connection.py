import pytest
from unittest.mock import Mock, patch
import confluent_kafka
from data_access_layer.connections import KafkaProducerConnection
from data_access_layer.connections.exceptions.connection import MissingConfigurationKey

import re


class TestKafkaProducerConnection:
    @pytest.fixture
    def connection_config(self):
        return {
            "type": "kafka_producer",
            "broker": "test_broker",
            "topic": "test_topic",
            "name": "test_kafka_producer",
        }

    def test_connection_creation(self, connection_config):
        connection = KafkaProducerConnection.from_dict(connection_config)
        assert connection.name == "test_kafka_producer"
        assert connection._broker == "test_broker"
        assert connection._topic == "test_topic"

    @patch('confluent_kafka.Producer')
    def test_connection_success(self, mock_kafka_producer, connection_config):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer

        connection = KafkaProducerConnection.from_dict(connection_config)
        connection.connect()

        mock_kafka_producer.assert_called_once_with({
            'bootstrap.servers': 'test_broker',
        })

    @patch('confluent_kafka.Producer')
    def test_connection_failure(self, mock_kafka_producer, connection_config):
        mock_kafka_producer.side_effect = confluent_kafka.KafkaException("Failed to connect")

        connection = KafkaProducerConnection.from_dict(connection_config)
        with pytest.raises(confluent_kafka.KafkaException):
            connection.connect()

    @patch('confluent_kafka.Producer')
    def test_disconnect(self, mock_kafka_producer, connection_config):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer

        connection = KafkaProducerConnection.from_dict(connection_config)
        connection.connect()

        connection.disconnect()
        assert connection._connection_engine is None

    @patch('confluent_kafka.Producer')
    def test_connection_health_check(self, mock_kafka_producer, connection_config):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer

        mock_producer.list_topics.return_value = Mock()
        
        connection = KafkaProducerConnection.from_dict(connection_config)
        connection.connect()

        assert connection.check_health() is True
        mock_producer.list_topics.assert_called_once_with(timeout=10000)

    def test_postgresql_connection_missing_required(self):
        incomplete_config = {
            "type": "kafka_producer",
            "broker": "test_broker",
            "topic": "test_topic",
        }

        with pytest.raises(MissingConfigurationKey):
            KafkaProducerConnection.from_dict(incomplete_config)

    def test_kafka_producer_connection_valid_config(self):
        valid_config = {
            "type": "kafka_producer",
            "broker": "test_broker",
            "topic": "test_topic",
            "name": "test_kafka_producer",
        }

        connection = KafkaProducerConnection.from_dict(valid_config)
        assert connection.name == "test_kafka_producer"
        assert connection._broker == "test_broker"
        assert connection._topic == "test_topic"
