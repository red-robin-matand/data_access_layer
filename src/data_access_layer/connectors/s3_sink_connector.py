from fastavro import reader
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
        self.n_messages = 1000

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
            batch_to_write = self._buffer.flush()
            self._sink.write_messages_to_parquet(
                records=batch_to_write,
                prefix=self.dataset_root,
                partition_cols=self.partition_columns,
            )

    def source_to_sink(self):

        while True:
            batch = self._source.consume(
                n_messages=self.n_messages,
                timeout=500,
            )
            for message in batch:
                self.handle_message(message=message)
