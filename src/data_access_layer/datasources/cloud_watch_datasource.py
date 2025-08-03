from data_access_layer.datasources import DataSource
from data_access_layer.connections import CloudWatchConnection
from data_access_layer.datasources.exceptions import CloudWatchDatasourceError

import pandas as pd


class CloudWatchDataSource(DataSource):

    def __init__(self, connection: CloudWatchConnection):
        super().__init__(connection)

    def put_metric_data(self, namespace : str, metric_data : list[dict]) -> None:

        try:
            self._connection_engine.put_metric_data(
                Namespace=namespace,
                MetricData=metric_data,
            )

        except Exception as e:
            raise CloudWatchDatasourceError(
                f"Failed to put metric data: {str(e)}")

            
