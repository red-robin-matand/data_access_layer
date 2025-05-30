
from data_access_layer.connectors import Connector


class SourceConnector(Connector):

    def __init__(self, name: str, source_name: str, sink_name: str) -> None:
        super().__init__(
            name=name,
            source_name=source_name,
            sink_name=sink_name,
        )
