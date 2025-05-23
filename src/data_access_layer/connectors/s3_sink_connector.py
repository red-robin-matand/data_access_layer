from fastavro import schemaless_reader
import io

from data_access_layer.connectors import (
    SinkConnector,
    MessageBuffer,
)

from data_access_layer.datasources import (
    S3DataSource,
    KafkaConsumerDataSource,
)


class S3SinkConnector(SinkConnector):

    def __init__(self, name: str, kafka_consumer_connection_name: str, s3_connection_name: str, schema: dict, buffer_args: dict,
                 partition_columns: list, dataset_root: str) -> None:

        super().__init__(
            name=name,
            source_name=kafka_consumer_connection_name,
            sink_name=s3_connection_name,
        )

        self._source: KafkaConsumerDataSource = None
        self._sink: S3DataSource = None
        self._buffer: MessageBuffer = None

        self.schema = schema
        self.buffer_args = buffer_args
        self.partition_columns = partition_columns
        self.dataset_root = dataset_root

        self.get_buffer()

    def get_buffer(self) -> None:
        self._buffer = MessageBuffer(**self.buffer_args)

    def decode_avro(self, value: bytes) -> dict:
        return schemaless_reader(io.BytesIO(value), self.schema)

    def handle_message(self, message: bytes) -> None:
        record = self.decode_avro(message.value)
        self._buffer.add(record)

        if self._buffer.should_flush():
            batch_to_write = self._buffer.flush()
            self._sink.write_messages_to_parquet(
                records=batch_to_write,
                root_path=self.dataset_root,
                partition_cols=self.partition_columns,
            )

    def run(self):

        while True:
            batch = self._source.poll(timeout_ms=500)
            for tp, messages in batch.items():
                for message in messages:
                    self.handle_message(message=message)
