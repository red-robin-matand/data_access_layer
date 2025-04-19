import pytest
from unittest.mock import Mock, patch
import confluent_kafka
from data_access_layer.connections import KafkaConsumerConnection
from data_access_layer.connections.exceptions.connection import MissingConfigurationKey

import re


class TestKafkaConsumerConnection:
    @pytest.fixture
    def connection_config(self):
        return {
            "type": "kafka_consumer",
            "broker": "test_broker",
            "topic": "test_topic",
            "name": "test_kafka_consumer",
            "group_id": "test_group",
            "offset": "earliest",
        }

    def test_connection_creation(self, connection_config):
        connection = KafkaConsumerConnection.from_dict(connection_config)
        assert connection.name == "test_kafka_consumer"
        assert connection._broker == "test_broker"
        assert connection._topic == "test_topic"
        assert connection._group_id == "test_group"
        assert connection._offset == "earliest"

    @patch('confluent_kafka.Consumer')
    def test_connection_success(self, mock_kafka_consumer, connection_config):
        mock_consumer = Mock()
        mock_kafka_consumer.return_value = mock_consumer

        connection = KafkaConsumerConnection.from_dict(connection_config)
        connection.connect()

        mock_kafka_consumer.assert_called_once_with({
            'bootstrap.servers': 'test_broker',
            'group.id': 'test_group',
            'auto.offset.reset': 'earliest'
        })

        mock_consumer.subscribe.assert_called_once_with(['test_topic'])

    @patch('confluent_kafka.Consumer')
    def test_connection_failure(self, mock_kafka_consumer, connection_config):
        mock_kafka_consumer.side_effect = confluent_kafka.KafkaException("Failed to connect")

        connection = KafkaConsumerConnection.from_dict(connection_config)
        with pytest.raises(confluent_kafka.KafkaException):
            connection.connect()

    @patch('confluent_kafka.Consumer')
    def test_disconnect(self, mock_kafka_consumer, connection_config):
        mock_consumer = Mock()
        mock_kafka_consumer.return_value = mock_consumer

        connection = KafkaConsumerConnection.from_dict(connection_config)
        connection.connect()

        connection.disconnect()
        mock_consumer.close.assert_called_once()
        assert connection._connection_engine is None

    @patch('confluent_kafka.Consumer')
    def test_connection_health_check(self, mock_kafka_consumer, connection_config):
        mock_consumer = Mock()
        mock_kafka_consumer.return_value = mock_consumer

        mock_consumer.list_topics.return_value = Mock()
        
        connection = KafkaConsumerConnection.from_dict(connection_config)
        connection.connect()

        assert connection.check_health() is True
        mock_consumer.list_topics.assert_called_once_with(timeout=10000)

    def test_postgresql_connection_missing_required(self):
        incomplete_config = {
            "type": "kafka_consumer",
            "broker": "test_broker",
            "topic": "test_topic",
        }

        with pytest.raises(MissingConfigurationKey):
            KafkaConsumerConnection.from_dict(incomplete_config)

    def test_kafka_consumer_connection_valid_config(self):
        valid_config = {
            "type": "kafka_consumer",
            "broker": "test_broker",
            "topic": "test_topic",
            "name": "test_kafka_consumer",
            "group_id": "test_group",
            "offset": "earliest",
        }

        connection = KafkaConsumerConnection.from_dict(valid_config)
        assert connection.name == "test_kafka_consumer"
        assert connection._broker == "test_broker"
        assert connection._topic == "test_topic"
        assert connection._group_id == "test_group"
        assert connection._offset == "earliest"
