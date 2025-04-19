import pytest
import pandas as pd
from unittest.mock import Mock, patch
from sqlalchemy.orm import Session
from data_access_layer.datasources import PostgreSQLDataSource
from data_access_layer.connections import PostgreSQLConnection
from data_access_layer.datasources.exceptions import OLTPDatasourceError


class TestPostgreSQLDataSource:
    @pytest.fixture
    def mock_connection(self):
        connection = Mock(spec=PostgreSQLConnection)
        connection.get_new_session.return_value = Mock(spec=Session)
        return connection

    @pytest.fixture
    def datasource(self, mock_connection):
        return PostgreSQLDataSource(connection=mock_connection)

    def test_insert_success(self, datasource, mock_connection):
        mock_session = mock_connection.get_new_session.return_value
        mock_model = Mock()
        datasource.get_model = Mock(return_value=mock_model)
        data = {"column1": "value1", "column2": "value2"}

        datasource.insert("test_entity", data)

        mock_model.assert_called_once_with(**data)
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_update_with_conditions(self, datasource, mock_connection):

        mock_session = mock_connection.get_new_session.return_value
        mock_instance = Mock()
        mock_session.query().filter().all.return_value = [mock_instance]
        data = {"column1": "new_value"}
        conditions = {"id": {"equal": [1]}}

        result = datasource.update(
            "test_entity", data, structured_conditions=conditions)

        assert result is True
        assert mock_instance.column1 == "new_value"
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    def test_query_execution(self, datasource, mock_connection):
        mock_session = mock_connection.get_new_session.return_value
        mock_session.bind = Mock()
        expected_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        
        with patch("pandas.read_sql", return_value=expected_df) as mock_read_sql:
            result = datasource.query("SELECT * FROM test_table")
            
        assert result.equals(expected_df)
        mock_read_sql.assert_called_once_with("SELECT * FROM test_table", mock_session.bind)
        mock_session.close.assert_called_once()

    def test_remove_with_conditions(self, datasource, mock_connection):
        mock_session = mock_connection.get_new_session.return_value
        mock_model = Mock()
        datasource.get_model = Mock(return_value=mock_model)
        mock_query = Mock()
        mock_session.query.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.delete.return_value = 1

        conditions = {"id": {"equal": [1]}}

        result = datasource.remove("test_entity", structured_conditions=conditions)

        assert result is True
        mock_query.delete.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()
