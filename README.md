# Data Access Layer

A Python library that provides a unified interface for connecting to different data sources: object stores (S3), streaming platforms (Kafka), and relational databases (PostgreSQL). It uses a factory pattern to create connections based on YAML configuration.

## Installation

```sh
pip install data_access_layer
```

## Getting Started

1. Set up your connections configuration in a YAML file:

```yaml
connections:
  - name: postgresql_name
    type: postgresql
    username: YOUR_USERNAME
    password: YOUR_PASSWORD
    host: YOUR_HOST
    database: YOUR_DATABASE
    ssl:
      ca: PATH_TO_YOUR_CA_CERTIFICATE
      cert: PATH_TO_YOUR_CLIENT_CERTIFICATE  # Optional
      key: PATH_TO_YOUR_CLIENT_KEY  # Optional

  - name: s3_name
    type: s3
    access_key: YOUR_S3_ACCESS_KEY
    secret_key: YOUR_S3_SECRET_KEY
    region: YOUR_S3_REGION
    connections: N_CONNECTIONS
    bukcet: YOUR_BUCKET_NAME

  - name: kafka_consumer_name
    type: kafka_consumer
    broker: YOUR_BROKER
    topic: YOUR_TOPIC
    group_id: YOUR_GROUP
    offest: YOUR_OFFEST

  - name: kafka_producer_name
    type: kafka_producer
    broker: YOUR_BROKER
    topic: YOUR_TOPIC
    config:
      acks: YOUR_ACKNOWLEDGE
      retries: N_RETRIES
      retry_backoff_ms: YOUR_BACKOFF
      compression_type: YOUR_COMPRESSION

```

2. Set the environment variable for your config:

```sh
# For Windows
set CONNECTIONS_YAML=C:\path\to\your\connections.yaml

# For Linux/Mac
export CONNECTIONS_YAML=/path/to/your/connections.yaml
```

3. Use the DataSourceManager to interact with your data sources:

```python
from data_access_layer.manager import DataSourceManager

# Initialize the manager
manager = DataSourceManager()
pg_datasource = manager.get_data_source("my-postgres")

data_entity_key = "my_table"
data = {
  "column1" : value1,
  "column2" : value2,
}

pg_datasource.insert(
  data_entity_key=data_entity_key,
  data=data,
)
```

## Supported Connection / DataSource Types

- `s3`: Amazon S3 using boto3
- `kafka_producer`: Kafka producer using confluent-kafka
- `kafka_consumer`: Kafka consumer using confluent-kafka
- `postgresql`: PostgreSQL using SQLAlchemy. For detailed description see [OLTP.README.md](OLTP.README.md)



## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.