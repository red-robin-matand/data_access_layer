from data_access_layer.connectors import SinkConnector

from data_access_layer.datasources import (
    S3DataSource,
    KafkaConsumerDataSource,
)

class S3SinkConnector(SinkConnector):

    def __init__(self, name : str, source_name : str, sink_name: str) -> None:
        
        super().__init__(
            name=name,
            source_name=source_name,
            sink_name=sink_name,
        )

        self._source : KafkaConsumerDataSource = None
        self._sink : S3DataSource = None

    def source_to_sink(self, source_args: dict, sink_args: dict) -> None:
        
        n_messages = source_args["n_messages"]
        timeout = source_args["timeout"]

        messages = self._source.consume(
            n_messages=n_messages,
            timeout=timeout,
        )

        if not messages:
            return
        
        object_name = sink_args["object_name"]
        config = sink_args["config"]
        
        self._sink.write_messages_to_parquet(
            messages=messages,
            object_name=object_name,
            config=config,
        )
