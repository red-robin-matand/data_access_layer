from abc import ABC, abstractmethod

from data_access_layer.datasources import DataSource
from data_access_layer.connections import ObjectStoreConnection


class ObjectStoreDataSource(DataSource, ABC):

    def __init__(self, connection: ObjectStoreConnection):
        super().__init__(connection)

        self._bucket = connection._bucket

    @abstractmethod
    def get_object(self, object_name: str) -> str:
        pass

    @abstractmethod
    def put_object(self, object_name: str, content: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, file_path: str, object_name: str, file_size: int) -> None:
        pass

    @abstractmethod
    def download_file(self, object_name: str, download_path: str) -> None:
        pass

    @abstractmethod
    def list_files(self, prefix: str = '', list_all: bool = True) -> list:
        pass

    @abstractmethod
    def delete_file(self, object_name: str) -> None:
        pass

    @abstractmethod
    def file_exists(self, object_name: str) -> bool:
        pass

    @abstractmethod
    def read_file_metadata(self, object_name: str) -> dict:
        pass
