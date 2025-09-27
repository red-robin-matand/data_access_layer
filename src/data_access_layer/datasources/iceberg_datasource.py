import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import (
    And, Or, BooleanExpression,
    StartsWith, NotStartsWith, 
    EqualTo, NotEqualTo, GreaterThan, LessThan, GreaterThanOrEqual, LessThanOrEqual, 
    IsNull, In, NotIn, NotNull,
)
from data_access_layer.datasources import DataLakeDataSource
from data_access_layer.connections import IcebergGlueConnection
from data_access_layer.datasources.exceptions import DataLakeDatasourceError



class IcebergDataSource(DataLakeDataSource):

    def __init__(self, connection: IcebergGlueConnection):
        super().__init__(connection)
        self._connection_engine : Catalog
        self.allowed_data_types_for_append = [
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
        
    def _read_table(self, namespace: str, table_name: str, columns: list, filters: BooleanExpression, snapshot_id : str, limit: int) -> pa.Table:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            scan = table.scan(
                snapshot_id=snapshot_id,
            )
            if columns:
                scan = scan.select(columns)
            if filters:
                scan = scan.filter(filters)
            
            table = scan.to_arrow()
            if limit and len(table) > limit:
                table = table.slice(0, limit)
            return table
        
        except Exception as e:
            message = f"Error reading table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def _handle_structured_conditions(self, structured_conditions: dict) -> BooleanExpression:

        and_conditions = []
        or_conditions = []
        string_to_pyiceberg_operator = {
            'eq': EqualTo,
            'gt': GreaterThan,
            'lt': LessThan,
            'gte': GreaterThanOrEqual,
            'lte': LessThanOrEqual,
            'starts_with': StartsWith,
            'not_starts_with': NotStartsWith,
            'is_null': IsNull,
            'not_null': NotNull,
            'in': In,
            'not_in': NotIn,
            'neq': NotEqualTo,
        }
        for column_name, column_condition in structured_conditions['and'].items():
            for operator, value in column_condition.items():
                if operator not in string_to_pyiceberg_operator:
                    raise ValueError(f"Unsupported operator '{operator}' in structured_conditions.")
                pyiceberg_operator = string_to_pyiceberg_operator[operator]
                subcondition = pyiceberg_operator(column_name, value) 
                and_conditions.append(subcondition)
        
        for column_name, column_condition in structured_conditions['or'].items():
            for operator, value in column_condition.items():
                if operator not in string_to_pyiceberg_operator:
                    raise ValueError(f"Unsupported operator '{operator}' in structured_conditions.")
                pyiceberg_operator = string_to_pyiceberg_operator[operator]
                subcondition = pyiceberg_operator(column_name, value)
                or_conditions.append(subcondition)

        if len(and_conditions)>0 and len(or_conditions)>0:
            return And(*and_conditions, Or(*or_conditions))
        
        if len(and_conditions)>0:
            return And(*and_conditions)
        
        if len(or_conditions)>0:
            return Or(*or_conditions)
        
        raise ValueError("structured_conditions must contain at least one 'and' or 'or' condition.")
                
    def read_table(self, namespace: str, table_name: str, columns=None, structured_conditions : dict = None, filters: dict =None, snapshot_id=None, limit: int = 100) -> pa.Table:
        if (not structured_conditions is None) and (not filters is None):
            raise ValueError("Cannot use both structured_conditions and filters at the same time.")
        
        if not structured_conditions is None:
            filters = self._handle_structured_conditions(structured_conditions)

        return self._read_table(
            namespace=namespace,
            table_name=table_name,  
            columns=columns,
            filters=filters,
            snapshot_id=snapshot_id,
            limit=limit,
        )

    def validate_data(self, data) ->None:

        if not any(isinstance(data, dtype) for dtype in self.allowed_data_types_for_append):
            allowed_types = ', '.join([dtype.__name__ for dtype in self.allowed_data_types_for_append])
            raise ValueError(f"Data must be one of the following types: {allowed_types}. Got {type(data)} instead.")    

    def append_to_table(self, namespace: str, table_name: str, data: pa.Table) -> None:
        try:
            self.validate_data(data)
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            table.append(data)
            
        except Exception as e:
            message = f"Error appending data to table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)

    def _delete_from_table(self, namespace: str, table_name: str, delete_filter: BooleanExpression) -> None:
        try:
            table = self._connection_engine.load_table(f"{namespace}.{table_name}")
            table.delete(delete_filter=delete_filter)
        except Exception as e:
            message = f"Error deleting data from table '{namespace}.{table_name}' : {str(e)}"
            raise DataLakeDatasourceError(message)
        
    def delete_from_table(self, namespace: str, table_name: str, structured_conditions : dict = None, delete_filter: BooleanExpression = None) -> None:
        if (not structured_conditions is None) and (not delete_filter is None):
            raise ValueError("Cannot use both structured_conditions and delete_filter at the same time.")
        
        if not structured_conditions is None:
            delete_filter = self._handle_structured_conditions(structured_conditions)

        self._delete_from_table(
            namespace=namespace,
            table_name=table_name,
            delete_filter=delete_filter,
        )
