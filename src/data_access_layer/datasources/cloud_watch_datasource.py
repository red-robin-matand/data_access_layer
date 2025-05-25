from data_access_layer.datasources import ObjectStoreDataSource
from data_access_layer.connections import CloudWatchConnection
from data_access_layer.datasources.exceptions import CloudWatchDatasourceError

import pandas as pd


class CloudWatchDataSource(ObjectStoreDataSource):

    def __init__(self, connection: CloudWatchConnection):
        super().__init__(connection)

    def get_log_streams(self, log_group_name: str, prefix: str = None) -> dict:
        try:
            response = self._connection_engine.describe_log_streams(
                logGroupName=log_group_name,
                logStreamNamePrefix=prefix,
                startFromHead=False,
                )
            
            log_streams = response['logStreams']
            result = {}

            for stream in log_streams:
                name = stream['logStreamName']
                created_at = stream['creationTime']
                result[name] = created_at

            return result
        except Exception as e:
            raise CloudWatchDatasourceError(f"Failed to get log streams: {str(e)}")
        
    def get_log_events(self, log_group_name: str, log_stream_name: str) -> pd.DataFrame:
        try:
            response = self._connection_engine.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                startFromHead=False
            )
            
            events = response['events']
            data = []

            for event in events:
                message = event['message']
                timestamp = event['timestamp']
                data.append({'message': message, 'timestamp': timestamp})

            result = pd.DataFrame(data)
            result['timestamp'] = pd.to_datetime(result['timestamp'], unit='ms')

            result = result.sort_values(by='timestamp', ascending=True).reset_index(drop=True)

            return result
        except Exception as e:
            raise CloudWatchDatasourceError(f"Failed to get log events: {str(e)}")