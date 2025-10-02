from abc import ABC, abstractmethod

from data_access_layer.datasources import DataSource
from data_access_layer.connections import DataLakeConnection


class DataLakeDataSource(DataSource, ABC):

    def __init__(self, connection: DataLakeConnection):
        super().__init__(connection)

    @abstractmethod
    def list_namespaces(self) -> list:
        pass
    
    @abstractmethod
    def list_tables(self, namespace: str) -> list:
        pass

    @abstractmethod
    def get_schema(self, namespace: str, table_name: str) -> dict:
        pass
    
    @abstractmethod
    def get_table_info(self, namespace: str, table_name: str) -> dict:
        pass
    
    @abstractmethod
    def read_table(self, namespace: str, table_name: str, columns=None, filters=None, snapshot_id=None, limit: int = 100, return_pandas : bool =False):
        pass

    @abstractmethod
    def append_to_table(self, namespace: str, table_name: str, data, partition_cols=None) -> None:
        pass

