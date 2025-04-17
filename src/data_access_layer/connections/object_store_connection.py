from abc import ABC, abstractmethod


from data_access_layer.connections import Connection


class ObjectStoreConnection(Connection, ABC):

    def __init__(self, name: str, bucket: str = None):
        super().__init__(name)
        self._bucket = bucket

    @property
    def bucket_name(self) -> str:
        return self._bucket
