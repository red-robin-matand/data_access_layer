from abc import ABC, abstractmethod


from data_access_layer.connections import Connection


class DataLakeConnection(Connection, ABC):

    def __init__(self, name: str):
        super().__init__(name)
