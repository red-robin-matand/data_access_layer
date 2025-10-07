import pandas as pd
from fastavro import writer, parse_schema
from io import BytesIO
from tqdm.auto import tqdm
import os

from data_access_layer.connectors import SourceConnector
from data_access_layer.connectors.exceptions import ConnectorException

from data_access_layer.datasources import (
    S3DataSource,
    KafkaProducerDataSource,
)

from fastavro import reader
import io

class S3SourceConnector(SourceConnector):

    def __init__(self, name : str, s3_connection_name : str, kafka_producer_connection_name: str) -> None:
        
        super().__init__(
            name=name,
            source_name=s3_connection_name,
            sink_name=kafka_producer_connection_name,
        )

        self._source : S3DataSource = None
        self._sink : KafkaProducerDataSource = None

    def download_file(self, object_name : str, download_path : str) -> None:

        self._source.download_file(
            object_name=object_name,
            download_path=download_path,
        )

    def read_df_from_path(self, path: str) -> pd.DataFrame:

        extension = path.split('.')[-1]

        func_dict= {
            'parquet' : pd.read_parquet,
            'csv' : pd.read_csv
        }

        if extension not in list(func_dict.keys()):
            message = f'Unsupported file extension: {path}. Supported extensions are {list(func_dict.keys())}'
            raise ConnectorException(message)
        
        df = func_dict[extension](path)

        return df

    def build_messages_list_from_df_path(self, path : str, schema : dict, key_columns : list, partition_column : str) -> list:

        df = self.read_df_from_path(path=path)
        parsed_schema = parse_schema(schema)

        messages = []
        for row in tqdm(df.itertuples(index=False), desc='building messages', total=df.shape[0]):
            record = row._asdict()

            buffer = BytesIO()
            writer(buffer, parsed_schema, [record])
            avro_bytes = buffer.getvalue()

            key = ':'.join([str(record[col]) for col in key_columns])
            partition = int(record[partition_column] % self._sink._partitions)

            messages.append({
                "key": key,
                "value": avro_bytes,
                "partition": partition,
            })

        return messages
    
    def produce_messages_from_df_path_w_mod(self, path : str, schema : dict, key_columns : list, partition_column : str, mod_func : function) -> None:

        df = self.read_df_from_path(path=path)
        df = mod_func(df)
        parsed_schema = parse_schema(schema)

        messages = []
        for row in tqdm(df.itertuples(index=False), desc='building and producing messages', total=df.shape[0]):
            record = row._asdict()

            buffer = BytesIO()
            writer(buffer, parsed_schema, [record])
            avro_bytes = buffer.getvalue()

            key = ':'.join([str(record[col]) for col in key_columns])
            partition = int(record[partition_column] % self._sink._partitions)

            messages.append({
                "key": key,
                "value": avro_bytes,
                "partition": partition,
            })

            if len(messages) >= 10000:
                self._sink.batch_produce(
                    messages=messages,
                    back_pressure_threshold=10000,
                )
                messages = []

        if len(messages) > 0:
            self._sink.batch_produce(
                messages=messages,
                back_pressure_threshold=10000,
            )

    def produce_from_object(self, object_name : str, download_path : str, schema : dict, key_columns : list, partition_column : str) -> None:
        
        local_dir = os.path.dirname(download_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        self.download_file(
            object_name=object_name,
            download_path=download_path,
        )

        messages = self.build_messages_list_from_df_path(
            path=download_path,
            schema=schema,
            key_columns=key_columns,
            partition_column=partition_column,
        )

        self._sink.batch_produce(
            messages=messages,
            back_pressure_threshold=10000,
        )
    
    def produce_from_large_dataset_w_mod(self, object_name : str, download_path : str, schema : dict, key_columns : list, partition_column : str, mod_func : function) -> None:
        
        local_dir = os.path.dirname(download_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        self.download_file(
            object_name=object_name,
            download_path=download_path,
        )

        self.produce_messages_from_df_path_w_mod(
            path=download_path,
            schema=schema,
            key_columns=key_columns,
            partition_column=partition_column,
            mod_func=mod_func,
        )
