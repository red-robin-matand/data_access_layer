from abc import ABC, abstractmethod
import pandas as pd

from data_access_layer.datasources import DataSource
from data_access_layer.connections import OLTPConnection


class OLTPDataSource(DataSource, ABC):

    def __init__(self, connection: OLTPConnection):
        super().__init__(connection)

    def register_model(self, model):
        self._connection.register_model(model)

    def reflect_tables(self, *table_names):
        self._connection.reflect_tables(table_names)

    def create_all_user_defined_models(self):
        self._connection.create_all_user_defined_models()

    def get_model(self, table_name):
        self.reflect_tables(table_name)

        model = self._connection.automap_base_model.classes.get(table_name)
        if model is None:
            raise ValueError(f"No model found for table: {table_name}")

        return model

    def get_new_session(self):
        return self._connection.get_new_session()

    @property
    def declarative_base_model(self):
        return self._connection.declarative_base_model

    @property
    def automap_base_model(self):
        return self._connection._automap_base_model

    @abstractmethod
    def insert(self, data_entity_key: str, data: dict) -> None:
        pass

    @abstractmethod
    def update(self, data_entity_key: str, data: dict, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        pass

    @abstractmethod
    def remove(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        pass

    @abstractmethod
    def query(self, query_string: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def find_all(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None, columns: list = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def exists(self, data_entity_key: str, structured_conditions: dict = None, raw_condition: str = None) -> bool:
        pass
