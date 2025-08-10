from data_access_layer.datasources import DataSource
from data_access_layer.connections import CloudWatchConnection
from data_access_layer.datasources.exceptions import CloudWatchDatasourceError

import pandas as pd


class CloudWatchDataSource(DataSource):

    def __init__(self, connection: CloudWatchConnection):
        super().__init__(connection)

    def chunk_metrics(self, metric_data : list, chunk_size: int):
        for i in range(0, len(metric_data), chunk_size):
            yield metric_data[i:i + chunk_size]

    def put_metric_data(self, namespace : str, metric_data : list[dict], chunk_size : int = 20) -> None:
        
        for chunk in self.chunk_metrics(
            metric_data=metric_data,
            chunk_size=chunk_size
            ):
            try:
                self._connection_engine.put_metric_data(
                    Namespace=namespace,
                    MetricData=chunk,
                )

            except Exception as e:
                raise CloudWatchDatasourceError(
                    f"Failed to put metric data: {str(e)}")

            
