from data_access_layer.datasources import ObjectStoreDataSource
from data_access_layer.connections import S3Connection
from data_access_layer.datasources.exceptions import ObjectStoreDatasourceError

from tqdm.auto import tqdm
from boto3.s3.transfer import TransferConfig, S3Transfer
import os
import pyarrow as pa
import pyarrow.parquet as pq


class S3DataSource(ObjectStoreDataSource):

    def __init__(self, connection: S3Connection):
        super().__init__(connection)

        self._bucket = connection._bucket
        self._region = connection._region

        self._large_file_transfer_config: TransferConfig = None
        self._huge_file_transfer_config: TransferConfig = None

        self._small_file_threshold: int = None
        self._large_file_threshold: int = None
        self._huge_file_threshold: int = None
        self._max_records_for_pandas: int = None
        self._allowable_operators: list = None

        self._set_file_thresholds()
        self._set_transfer_configs()
        self._set_read_dataset_config()

        self.fs = pa.fs.S3FileSystem(region=self._region)

    def _set_file_thresholds(self) -> None:

        self._small_file_threshold = 1024 * 1024 * 5
        self._large_file_threshold = 1024 * 1024 * 100  # 100 MB
        self._huge_file_threshold = 1024 * 1024 * 500  # 500 MB

    def _set_transfer_configs(self) -> None:

        mb = 1024 * 1024
        multipart_chunksize = 10 * mb
        max_concurrency = 10
        use_threads = True

        self._large_file_transfer_config = TransferConfig(
            multipart_threshold=5*mb,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            use_threads=use_threads,
        )

        self._huge_file_transfer_config = TransferConfig(
            multipart_threshold=100*mb,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            use_threads=use_threads,
        )

    def _set_read_dataset_config(self) -> None:

        self._max_records_for_pandas = 1000000
        self._allowable_operators = ['=', '!=', '<', '<=', '>', '>=', 'in', 'not in']

    def get_object(self, object_name: str) -> str:
        try:
            response = self._connection_engine.get_object(
                Bucket=self._bucket, Key=object_name)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            message = f"Error getting object {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def put_object(self, object_name: str, content: str) -> None:
        try:
            self._connection_engine.put_object(
                Bucket=self._bucket, Key=object_name, Body=content)
        except Exception as e:
            message = f"Error putting object {object_name} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def upload_file(self, file_path: str, object_name: str) -> None:

        file_size = os.path.getsize(file_path)

        if file_size <= self._small_file_threshold:

            self._upload_small_file(
                file_path=file_path,
                object_name=object_name,
            )
            return

        if file_size <= self._large_file_threshold:
            self._upload_large_file(
                file_path=file_path,
                object_name=object_name,
            )
            return

        self._upload_huge_file(
            file_path=file_path,
            object_name=object_name,
            file_size=file_size,
        )

    def download_file(self, object_name: str, download_path: str) -> None:

        file_size = self.get_object_size(object_name)

        if file_size <= self._small_file_threshold:

            self._download_small_file(
                object_name=object_name,
                download_path=download_path,
            )
            return

        self._download_large_file(
            object_name=object_name,
            download_path=download_path,
        )

    def list_files(self, prefix: str = '', list_all: bool = True) -> list:
        try:
            if list_all:
                paginator = self._connection_engine.get_paginator(
                    'list_objects_v2')
                operation_parameters = {
                    'Bucket': self._bucket,
                    'Prefix': prefix
                }
                files = []
                for page in tqdm(paginator.paginate(**operation_parameters), desc=f"Listing files with prefix {prefix}"):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            files.append(obj['Key'])
                return files
            else:
                response = self._connection_engine.list_objects_v2(
                    Bucket=self._bucket,
                    Prefix=prefix,
                )
                files = [obj['Key'] for obj in response.get('Contents', [])]
                return files
        except Exception as e:
            message = f"Error listing files with prefix {prefix}: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def delete_file(self, object_name: str) -> None:
        try:
            self._connection_engine.delete_object(
                Bucket=self._bucket, Key=object_name)
            print(f"File {self._bucket}/{object_name} deleted.")
        except Exception as e:
            message = f"Error deleting file {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def file_exists(self, object_name: str) -> bool:
        try:
            self._connection_engine.head_object(
                Bucket=self._bucket, Key=object_name)
            return True
        except Exception as e:
            if "Not Found" in str(e):
                return False
            else:
                message = f"Error checking existence of file {object_name} in S3: {str(e)}"
                raise ObjectStoreDatasourceError(message)

    def read_file_metadata(self, object_name: str) -> dict:
        try:
            metadata = self._connection_engine.head_object(
                Bucket=self._bucket, Key=object_name)
            result = {
                'size_in_bytes': metadata['ContentLength'],
                'modified': metadata['LastModified'],
            }
            return result
        except Exception as e:
            message = f"Error reading metadata for file {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def _upload_small_file(self, file_path: str, object_name: str) -> None:
        try:
            self._connection_engine.upload_file(
                file_path,
                self._bucket,
                object_name,
            )
        except Exception as e:
            message = f"Error uploading small file {file_path} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def _upload_large_file(self, file_path: str, object_name: str) -> None:
        try:
            self._connection_engine.upload_file(
                file_path,
                self._bucket,
                object_name,
                Config=self._large_file_transfer_config,
            )
        except Exception as e:
            message = f"Error uploading large file {file_path} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def _upload_huge_file(self, file_path: str, object_name: str, file_size: int) -> None:
        try:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Uploading") as progress_bar:
                def progress_callback(bytes_transferred):
                    progress_bar.update(bytes_transferred)

                s3_transfer = S3Transfer(
                    self._connection_engine, self._huge_file_transfer_config)
                s3_transfer.upload_file(
                    file_path,
                    self._bucket,
                    object_name,
                    callback=progress_callback,
                )
        except Exception as e:
            message = f"Error uploading huge file {file_path} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def _download_small_file(self, object_name: str, download_path: str) -> None:
        try:
            self._connection_engine.download_file(
                self._bucket,
                object_name,
                download_path,
            )
        except Exception as e:
            message = f"Error downloading small file {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def _download_large_file(self, object_name: str, download_path: str) -> None:
        try:
            transfer = S3Transfer(self._connection_engine,
                                  self._large_file_transfer_config)
            transfer.download_file(
                self._bucket,
                object_name,
                download_path,
            )
        except Exception as e:
            message = f"Error downloading large file {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def get_object_size(self, object_name: str) -> int:
        try:
            metadata = self._connection_engine.head_object(
                Bucket=self._bucket, Key=object_name)
            return metadata['ContentLength']
        except Exception as e:
            message = f"Error getting size of object {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def write_messages_to_parquet(self, records: list, prefix: str, partition_cols: list) -> None:
        try:
            compression = 'snappy'
            row_group_size = 1000000
            root_path = f'{self._bucket}/{prefix}'

            table = pa.Table.from_pylist(records)
            pq.write_to_dataset(
                table,
                root_path,
                filesystem=self.fs,
                partition_cols=partition_cols,
                compression=compression,
                row_group_size=row_group_size,
            )
        except Exception as e:
            message = f"Error writing table {root_path} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def validate_filters(self, filters: list) -> None:
        if not isinstance(filters, list):
            raise ValueError("Filters must be a list of tuples.")
        for item in filters:
            if not (isinstance(item, tuple) and len(item) == 3):
                raise ValueError("Each filter must be a tuple of (field, operator, value).")
            field, operator, value = item
            if not isinstance(field, str):
                raise ValueError(f"Field {field} must be a string. Got {type(field)}")
            if operator not in self._allowable_operators:
                raise ValueError(f"Operator {operator} for field {field} is not supported. Allowed operators are {self._allowable_operators}.")
            if operator in ['in', 'not in'] and not isinstance(value, list):
                raise ValueError(f"Value for operator '{operator}', which is set for field {field} must be a list. Got {type(value)}")
        
    def read_dataset(self, root_path: str, filters: list = None, return_pandas: bool = False) -> pa.Table:
        try:
            if not filters is None:
                self.validate_filters(filters)
            dataset = pq.ParquetDataset(
                root_path,
                filesystem=self.fs,
                filters=filters,
            )
            table = dataset.read()
            if return_pandas:
                n_records = table.num_rows
                if n_records > self._max_records_for_pandas:
                    raise ValueError(f"Dataset has {n_records} records which exceeds the maximum of {self._max_records_for_pandas} for conversion to pandas DataFrame.")
                return table.to_pandas()
            return table
        except Exception as e:
            message = f"Error reading dataset {root_path} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)
