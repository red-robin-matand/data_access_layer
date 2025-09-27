import io
import threading
from fastavro import reader
from data_access_layer.connectors import (
    SinkConnector,
    MessageBuffer,
)

from data_access_layer.datasources import (
    IcebergDataSource,
    KafkaConsumerDataSource,
)


class IcebergSinkConnector(SinkConnector):

    def __init__(self, name: str, kafka_consumer_connection_name: str,iceberg_glue_connection_name: str, schema: dict, buffer_args: dict,
                 partition_columns: list, namespace: str, table: str) -> None:

        super().__init__(
            name=name,
            source_name=kafka_consumer_connection_name,
            sink_name=iceberg_glue_connection_name,
        )

        self._source: KafkaConsumerDataSource = None
        self._sink: IcebergDataSource = None
        self._buffer: MessageBuffer = None

        self.schema = schema
        self.buffer_args = buffer_args
        self.partition_columns = partition_columns
        self.namespace = namespace
        self.table = table
        self.n_messages = 1000
        self._stop_event = threading.Event()

        self.get_buffer()

    def get_buffer(self) -> None:
        self._buffer = MessageBuffer(**self.buffer_args)

    def decode_avro(self, value: bytes) -> dict:
        record_iter = reader(io.BytesIO(value), self.schema)
        record = next(record_iter)
        return record

    def handle_message(self, message: bytes) -> None:
        record = self.decode_avro(message.value())
        self._buffer.add(record)

        if self._buffer.should_flush():
            data = self._buffer.flush_as_arrow_table()
            self._sink.append_to_table(
                namespace=self.namespace,
                table_name=self.table,
                data=data,
            )

    def source_to_sink(self):
        try:
            while not self._stop_event.is_set():
                batch = self._source.consume(
                    n_messages=self.n_messages,
                    timeout=500,  
                )
                for message in batch:
                    self.handle_message(message=message)
        except KeyboardInterrupt:
            print("Received Ctrl+C, stopping gracefully...")
            self._stop_event.set()
        finally:
            self.disconnect()

    def setup(self) -> None:
        
        self._sink.create_namespace(self.namespace)
        self._sink.create_table(
            namespace=self.namespace,
            table_name=self.table,
            schema=self.schema,
            partition_spec=self.partition_spec,
            properties=self.properties,
        )