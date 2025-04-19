
import pandas as pd
from sqlalchemy import text
from sqlalchemy.sql import text
from sqlalchemy.sql import and_, or_

from sqlalchemy.orm import Session, Query

from data_access_layer.connections import PostgreSQLConnection
from data_access_layer.datasources import OLTPDataSource
from data_access_layer.datasources.exceptions import OLTPDatasourceError


class PostgreSQLDataSource(OLTPDataSource):

    def __init__(self, connection: PostgreSQLConnection):
        super().__init__(connection)

    def _get_single_structured_condition(self, column, value):

        condition = None

        if isinstance(value, dict):
            if 'greater_than' in value:
                condition = column > value['greater_than']
            elif 'greater_than_or_equal' in value:
                condition = column >= value['greater_than_or_equal']
            elif 'between' in value:
                inclusive = value.get('inclusive', True)
                lower, upper = value['between']
                if inclusive:
                    condition = column.between(lower, upper)
                else:
                    condition = (column > lower) & (column < upper)
            elif 'less_than' in value:
                condition = column < value['less_than']
            elif 'less_than_or_equal' in value:
                condition = column <= value['less_than_or_equal']
            elif 'not_equal' in value:
                condition = column != value['not_equal']
            elif 'like' in value:
                condition = column.like(value['like'])
            elif 'ilike' in value:
                condition = column.ilike(value['ilike'])
        else:
            condition = column == value

        return condition

    def _handle_structured_conditions(self, query: Query, model, structured_conditions: dict) -> Query:

        for column_name, column_condition in structured_conditions.items():
            column = getattr(model, column_name)
            for operator, values in column_condition.items():
                conditions = []
                for value in values:

                    condition = self._get_single_structured_condition(
                        column, value)
                    if condition is not None:
                        conditions.append(condition)

                if conditions:
                    query = query.filter(
                        or_(*conditions) if operator == 'or' else and_(*conditions))

        return query

    def _prepare_query(self, session: Session, data_entity_key: str, structured_conditions=None, raw_condition=None) -> Query:
        model = self.get_model(data_entity_key)
        query = session.query(model)

        query = self._handle_structured_conditions(
            query=query,
            model=model,
            structured_conditions=structured_conditions,
        )

        if raw_condition:
            query = query.filter(text(raw_condition))

        return query

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
                session=session,
                data_entity_key=data_entity_key,
                structured_conditions=structured_conditions,
                raw_condition=raw_condition,
            )
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
                session=session,
                data_entity_key=data_entity_key,
                structured_conditions=structured_conditions,
                raw_condition=raw_condition,
            )
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
            session=session,
            data_entity_key=data_entity_key,
            structured_conditions=structured_conditions,
            raw_condition=raw_condition,
        )

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
            session=session,
            data_entity_key=data_entity_key,
            structured_conditions=structured_conditions,
            raw_condition=raw_condition,
        )
        try:
            return session.query(query.exists()).scalar()
        finally:
            session.close()

    def upsert(self, data_entity_key: str, data: dict, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        session = self.get_new_session()
        try:
            query = self._prepare_query(
                session, data_entity_key, structured_conditions, raw_condition)
            instance = query.first()
            if instance:
                for key, value in data.items():
                    setattr(instance, key, value)
            else:
                instance = self.get_model(data_entity_key)(**data)
                session.add(instance)
            session.commit()
            return True
        finally:
            session.close()

    def insert_dataframe(self, data_entity_key: str, data: pd.DataFrame) -> None:
