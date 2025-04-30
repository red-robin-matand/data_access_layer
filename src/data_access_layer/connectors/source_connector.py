from abc import ABC, abstractmethod

from data_access_layer.connectors import Connector


class SourceConnector(Connector, ABC):

    @abstractmethod
    def source_to_sink(self, source_args: dict, sink_args: dict) -> None:
        pass
