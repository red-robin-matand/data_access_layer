
import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import ResultProxy
from sqlalchemy.sql import text

from sqlalchemy.dialects.postgresql import insert

from data_access_layer.connections import PostgreSQLConnection
from data_access_layer.datasources import OLTPDataSource
from data_access_layer.datasources.exceptions import OLTPDatasourceError


class PostgreSQLDataSource(OLTPDataSource):

    def __init__(self, connection: PostgreSQLConnection):
        super().__init__(connection)

    def insert(self, data_entity_key: str, data: dict) -> None:
        session = self.get_new_session()
        try:
            instance = self.get_model(data_entity_key)(**data)
            session.add(instance)
            session.commit()
        except Exception as e:
            session.rollback()
            message = f"Error inserting data: {data}\ninto data entity key: {data_entity_key}\nError: {e}"
            raise OLTPDatasourceError(message)
        finally:
            session.close()

    def update(self, data_entity_key: str, data: dict, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        session = self.get_new_session()
        try:
            query = self._prepare_query(
                session, data_entity_key, structured_conditions, raw_condition)
            instances = query.all()
            if not instances:
                return False
            for instance in instances:
                for key, value in data.items():
                    setattr(instance, key, value)
            session.commit()
            return True
        finally:
            session.close()

    def remove(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        session = self.get_new_session()
        try:
            query = self._prepare_query(
                session, data_entity_key, structured_conditions, raw_condition)
            num_deleted = query.delete()
            session.commit()
            return num_deleted > 0
        finally:
            session.close()

    def query(self, query_string: str) -> pd.DataFrame:
        session = self.get_new_session()
        df = pd.read_sql(query_string, session.bind)
        session.close()
        return df

    def find_all(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None, columns: list = None) -> pd.DataFrame:
        session = self.get_new_session()
        query = self._prepare_query(
            session, data_entity_key, structured_conditions, raw_condition)

        if columns:
            model = self.get_model(data_entity_key)
            query = query.with_entities(
                *(getattr(model, col) for col in columns))
        
        df = pd.read_sql(query.statement, session.bind)
        session.close()
        return df


    def exists(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        session = self.get_new_session()
        query = self._prepare_query(
            session, data_entity_key, structured_conditions, raw_condition)
        try:
            return session.query(query.exists()).scalar()
        finally:
            session.close()
