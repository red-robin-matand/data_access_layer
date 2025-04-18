from data_access_layer.datasources import ObjectStoreDataSource
from data_access_layer.connections import S3Connection
from data_access_layer.datasources.exceptions import ObjectStoreDatasourceError

from tqdm.auto import tqdm


class S3DataSource(ObjectStoreDataSource):

    def __init__(self, connection: S3Connection):
        super().__init__(connection)

        self._bucket = connection._bucket

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
        try:
            self._connection_engine.upload_file(
                file_path,
                self._bucket,
                object_name,
            )
        except Exception as e:
            message = f"Error uploading file {file_path} to S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

    def download_file(self, object_name: str, download_path: str) -> None:
        try:
            self._connection_engine.download_file(
                self._bucket,
                object_name,
                download_path
            )
        except Exception as e:
            message = f"Error downloading file {object_name} from S3: {str(e)}"
            raise ObjectStoreDatasourceError(message)

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
