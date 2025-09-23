import pandas as pd
import pyarrow as pa
from typing import Union
from pyiceberg.catalog import Catalog
from data_access_layer.datasources import DataLakeDataSource
from data_access_layer.connections import IcebergGlueConnection
from data_access_layer.datasources.exceptions import DataLakeDatasourceError



class IcebergGlueDataSource(DataLakeDataSource):

    def __init__(self, connection: IcebergGlueConnection):
        super().__init__(connection)
        self._connection_engine : Catalog
        self.allowed_data_types_for_append = [
            pd.DataFrame,
            pa.Table,
        ]
        
    def list_namespaces(self) -> list:
        try:
            return self._connection_engine.list_namespaces()
        except Exception as e:
            message = f"Error listing namespaces: {str(e)}"
            raise DataLakeDatasourceError(message)

    def create_namespace(self, namespace: str) -> None:
        try:
            self._connection_engine.create_namespace(namespace)
        except Exception as e:
            message = f"Error creating namespace '{namespace}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def list_tables(self, namespace: str) -> list:
        try:
            return self._connection_engine.list_tables(namespace)
        except Exception as e:
            message = f"Error listing tables for namesapce: '{namespace}' : {str(e)}"
            raise DataLakeDatasourceError(message)
        
    def get_schema(self, namespace: str, table_name: str) -> dict:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            return table.schema().to_dict()
        except Exception as e:
            message = f"Error getting schema for table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def create_table(self, namespace: str, table_name: str, schema: dict, partition_spec: dict = None, properties: dict = None) -> None:
        try:
            self._connection_engine.create_table(
                identifier=f"{namespace}.{table_name}",
                schema=schema,
                partition_spec=partition_spec,
                properties=properties
            )
        except Exception as e:
            message = f"Error creating table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def delete_table(self, namespace: str, table_name: str) -> None:
        try:
            self._connection_engine.drop_table(f"{namespace}.{table_name}")
        except Exception as e:
            message = f"Error deleting table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def get_table_info(self, namespace: str, table_name: str) -> dict:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            return {
                "name": table.name,
                "location": table.location,
                "schema": table.schema().to_dict(),
                "spec": table.spec().to_dict(),
                "properties": table.properties,
            }
        except Exception as e:
            message = f"Error getting table '{namespace}.{table_name}' info : {str(e)}"
            raise DataLakeDatasourceError(message)
        
    def read_table(self, namespace: str, table_name: str, columns=None, filters=None, snapshot_id=None, limit: int = 100, return_pandas : bool =False) -> Union[pa.Table,pd.DataFrame]:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            scan = table.scan()
            if snapshot_id:
                scan = scan.at_snapshot(snapshot_id)
            if columns:
                scan = scan.select(columns)
            if filters:
                scan = scan.filter(filters)

            if return_pandas:
                df = scan.to_pandas()
                if limit and len(df) > limit:
                    df = df.head(limit)
                return df
            
            table = scan.to_arrow()
            if limit and len(table) > limit:
                table = table.slice(0, limit)
            return table
        
        except Exception as e:
            message = f"Error reading table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def validate_data(self, data) ->None:

        if not any(isinstance(data, dtype) for dtype in self.allowed_data_types_for_append):
            allowed_types = ', '.join([dtype.__name__ for dtype in self.allowed_data_types_for_append])
            raise ValueError(f"Data must be one of the following types: {allowed_types}. Got {type(data)} instead.")    

    def append_to_table(self, namespace: str, table_name: str, data: Union[pd.DataFrame, pa.Table], partition_cols=None) -> None:
        try:
            self.validate_data(data)
            
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            if isinstance(df, pd.DataFrame):
                df = pa.Table.from_pandas(df)
            
            writer = table.new_append()
            writer.append_dataframe(df)
            writer.commit()
        except Exception as e:
            message = f"Error appending data to table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def overwrite_partitions(self, namespace: str, table_name: str, data: Union[pd.DataFrame, pa.Table], partition_filter: dict) -> None:
        try:
            self.validate_data(data)

            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            if isinstance(df, pd.DataFrame):
                df = pa.Table.from_pandas(df)
            
            writer = table.new_replace_partitions()
            writer.overwrite_partition(partition_filter, df)
            writer.commit()
        except Exception as e:
            message = f"Error overwriting partitions in table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def overwrite_table(self, namespace: str, table_name: str, data: Union[pd.DataFrame, pa.Table]) -> None:
        try:
            self.validate_data(data)

            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            if isinstance(df, pd.DataFrame):
                df = pa.Table.from_pandas(df)
            
            writer = table.new_overwrite()
            writer.overwrite(df)
            writer.commit()
        except Exception as e:
            message = f"Error overwriting table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def add_columns_to_schema(self, table, new_fields: list) -> None:
        for field in new_fields:
            try:
                table.update_schema().add_field(field).commit()
            except Exception as e:
                message = f"Error adding field '{field['name']}' to schema: {str(e)}"
                raise DataLakeDatasourceError(message)

    def remove_columns_from_schema(self, table, fields_to_remove: list) -> None:
        for field in fields_to_remove:
            try:
                table.update_schema().remove_field(field['name']).commit()
            except Exception as e:
                message = f"Error removing field '{field['name']}' from schema: {str(e)}"
                raise DataLakeDatasourceError(message)

    def evolve_table_schema(self, namespace: str, table_name: str, new_schema: dict) -> None:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            current_schema = table.schema().to_dict()

            new_fields = list(set(new_schema['fields']) - set(current_schema['fields']))
            if new_fields:
                self.add_columns_to_schema(table, new_fields)

            fields_to_remove = list(set(current_schema['fields']) - set(new_schema['fields']))
            if fields_to_remove:
                self.remove_columns_from_schema(table, fields_to_remove)

        except Exception as e:
            message = f"Error evolving schema for table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)
    
    def expire_snapshots(self, namespace: str, table_name: str, older_than_timestamp: int) -> None:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            table.expire_snapshots(older_than_timestamp)
        except Exception as e:
            message = f"Error expiring snapshots for table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)
        
    def compact_table(self, namespace: str, table_name: str) -> None:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            table.rewrite_manifests()
        except Exception as e:
            message = f"Error compacting table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)
