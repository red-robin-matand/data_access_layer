# OLTP Data Sources

Documentation for OLTP (Online Transaction Processing) data sources in the data access layer. This covers the base `OLTPDataSource` class and its implementations like `PostgreSQLDataSource`.

## Core Features

### Connect to data source

```python
from data_access_layer.manager import DataSourceManager

manager = DataSourceManager()
datasource = manager.get_data_source("my-postgres")

```

### Data Operations

#### Insert Data

```python
# Single record insertion
datasource.insert(
    data_entity_key="users",
    data={
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    }
)
```

#### Update Data

```python
# Update with structured conditions
datasource.update(
    data_entity_key="users",
    data={"status": "inactive"},
    structured_conditions={
        "last_login": {
            "or": {
                "less_than": "2024-01-01"
            }
        }
    }
)

# Update with raw condition
datasource.update(
    data_entity_key="users",
    data={"status": "active"},
    raw_condition="age >= 18 AND email LIKE '%@company.com'"
)
```

#### Remove Data

```python
# Remove with structured conditions
datasource.remove(
    data_entity_key="users",
    structured_conditions={
        "status": {
            "or": ["inactive"]
        }
    }
)

# Remove with raw condition
datasource.remove(
    data_entity_key="users",
    raw_condition="created_at < NOW() - INTERVAL '1 year'"
)
```

#### Query Data

```python
# Raw SQL query returning pandas DataFrame
df = datasource.query(
    "SELECT name, COUNT(*) as order_count " +
    "FROM users JOIN orders ON users.id = orders.user_id " +
    "GROUP BY name"
)

# Find all records with conditions
df = datasource.find_all(
    data_entity_key="users",
    structured_conditions={
        "age": {
            "and": {
                "greater_than": 21
            }
        },
        "status": {
            "and": ["active"]
        }
    },
    columns=["id", "name", "email"]
)
```

#### Check Existence

```python
# Check if records exist
exists = datasource.exists(
    data_entity_key="users",
    structured_conditions={
        "email": {
            "like": {
                "like": "%@company.com"
            }
        }
    }
)
```

### Structured Conditions

The datasource supports complex query conditions through the `structured_conditions` parameter:

```python
structured_conditions = {
    "age": {
        "or": [
            {"greater_than": 21},
            {"between": [18, 21]}
        ]
    },
    "status": {
        "and": [
            {"equal": "active"},
            {"not_equal": "deleted"}
        ]
    }
}
```

Supported condition operators:
- `equal`: Exact match
- `not_equal`: Not equal to value
- `greater_than`: Greater than value
- `greater_than_or_equal`: Greater than or equal to value
- `less_than`: Less than value
- `less_than_or_equal`: Less than or equal to value
- `between`: Between two values (inclusive or exclusive)
- `like`: SQL LIKE pattern matching
- `ilike`: Case-insensitive LIKE pattern matching


## Best Practices

1. Always use structured conditions instead of raw conditions when possible
2. Use specific columns in find_all to optimize query performance


For more details, refer to the source code documentation and implementation in `oltp_datasource.py` and `postgresql_datasource.py`.