import io
import threading
import pyarrow as pa
from fastavro import reader
from pyiceberg.schema import Schema, NestedField, StringType, LongType, DoubleType, BooleanType
from pyiceberg.types import FloatType, IntegerType
from data_access_layer.connectors import (
    SinkConnector,
    MessageBuffer,
)

from data_access_layer.datasources import (
    IcebergDataSource,
    KafkaConsumerDataSource,
)


class IcebergSinkConnector(SinkConnector):

    def __init__(self, name: str, kafka_consumer_connection_name: str,iceberg_connection_name: str, schema: dict, buffer_args: dict,
                 partition_columns: list, namespace: str, table: str) -> None:

        super().__init__(
            name=name,
            source_name=kafka_consumer_connection_name,
            sink_name=iceberg_connection_name,
        )

        self._source: KafkaConsumerDataSource = None
        self._sink: IcebergDataSource = None
        self._buffer: MessageBuffer = None

        self.schema = schema
        self._pa_schema : pa.schema = None
        self._iceberg_schema : Schema = None
        self.buffer_args = buffer_args
        self.partition_columns = partition_columns
        self._partition_spec = None
        self.namespace = namespace
        self.table = table
        self.n_messages = 1000
        self._stop_event = threading.Event()

        self.get_pa_schema()
        self.get_iceberg_schema()
        self.get_buffer()

    def get_buffer(self) -> None:
        args = {
            "schema": self._pa_schema,
            **self.buffer_args,
        }
        self._buffer = MessageBuffer(**args)

    def get_pa_schema(self) -> None:
        fields = []
        for field in self.schema['fields']:
            name = field['name']
            avro_type = field['type']
            if isinstance(avro_type, list):
                if 'null' in avro_type:
                    avro_type.remove('null')
                    nullable = True
                else:
                    nullable = False
                avro_type = avro_type[0]
            else:
                nullable = False

            if avro_type == 'string':
                pa_type = pa.string()
            elif avro_type == 'int':
                pa_type = pa.int32()
            elif avro_type == 'long':
                pa_type = pa.int64()
            elif avro_type == 'float':
                pa_type = pa.float32()
            elif avro_type == 'double':
                pa_type = pa.float64()
            elif avro_type == 'boolean':
                pa_type = pa.bool_()
            elif isinstance(avro_type, dict) and avro_type.get('type') == 'array':
                item_type = self.convert_avro_schema_to_pyarrow({'fields': [{'name': 'item', 'type': avro_type['items']}]})[0].type
                pa_type = pa.list_(item_type)
            elif isinstance(avro_type, dict) and avro_type.get('type') == 'map':
                value_type = self.convert_avro_schema_to_pyarrow({'fields': [{'name': 'value', 'type': avro_type['values']}]})[0].type
                pa_type = pa.map_(pa.string(), value_type)
            else:
                raise ValueError(f"Unsupported Avro type: {avro_type}")

            fields.append(pa.field(name, pa_type, nullable=nullable))

        self._pa_schema = pa.schema(fields)

    def get_iceberg_schema(self) -> None:
        fields = []
        field_id = 1
        for field in self.schema['fields']:
            name = field['name']
            avro_type = field['type']
            
            if isinstance(avro_type, list):
                if 'null' in avro_type:
                    avro_type.remove('null')
                avro_type = avro_type[0]

            if avro_type == 'string':
                iceberg_type = StringType()
            elif avro_type == 'int':
                iceberg_type = IntegerType()
            elif avro_type == 'long':
                iceberg_type = LongType()
            elif avro_type == 'float':
                iceberg_type = FloatType()
            elif avro_type == 'double':
                iceberg_type = DoubleType()
            elif avro_type == 'boolean':
                iceberg_type = BooleanType()
            else:
                raise ValueError(f"Unsupported Avro type: {avro_type}")
            
            fields.append(NestedField(field_id, name, iceberg_type, True))
            field_id += 1

        self._iceberg_schema = Schema(*fields)

    def get_partition_spec(self) -> None:
        
        if len(self.partition_columns) == 0:
            return
        
        self._partition_spec = self._sink.get_partition_spec_from_partition_columns_and_schema(
            partition_columns=self.partition_columns,
            schema=self._iceberg_schema,
        )

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
                    timeout=5,  
                )
                if len(batch) == 0:
                    print("No messages received, waiting...")
                    continue
                for message in batch:
                    self.handle_message(message=message)
        except KeyboardInterrupt:
            print("Received Ctrl+C, stopping gracefully...")
            self._stop_event.set()
        finally:
            self.disconnect()

    def setup(self) -> None:
        
        namespaces = self._sink.list_namespaces()
        if self.namespace not in namespaces:
            self._sink.create_namespace(self.namespace)

        tables = self._sink.list_tables(self.namespace)
        if self.table not in tables:
            self._sink.create_table(
                namespace=self.namespace,
                table_name=self.table,
                schema=self._iceberg_schema,
                partition_spec=self._partition_spec,
            )