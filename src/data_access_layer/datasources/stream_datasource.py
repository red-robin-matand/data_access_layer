from abc import ABC, abstractmethod

from data_access_layer.datasources import DataSource
from data_access_layer.connections import StreamConnection


class StreamDataSource(DataSource, ABC):

    def __init__(self, connection: StreamConnection):
        super().__init__(connection)
