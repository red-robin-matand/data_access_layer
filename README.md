# Data Access Layer

A Python library that provides a unified interface for connecting to different data sources: object stores (S3), streaming platforms (Kafka), and relational databases (PostgreSQL). It uses a factory pattern to create connections, datasources and connectors based on YAML configuration.

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
    port: 5432
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
    bucket: YOUR_BUCKET_NAME

  - name: kafka_consumer_name
    type: kafka_consumer
    broker: YOUR_BROKER
    topic: YOUR_TOPIC
    group_id: YOUR_GROUP
    offset: earliest

  - name: kafka_producer_name
    type: kafka_producer
    broker: YOUR_BROKER
    topic: YOUR_TOPIC
    partitions: 6
    config:
      acks: all
      retries: 3
      retry.backoff.ms: 1000
      compression.type: snappy

connectors:
  - name: kafka_to_s3
    type: s3_sink
    source_name: kafka_consumer_connection
    sink_name: s3_connection
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
from data_access_layer.datasources import DataSourceManager

# Initialize the manager
manager = DataSourceManager()

# Get a data source by its connection name
s3 = manager.get_data_source("s3_connection")
kafka_consumer = manager.get_data_source("kafka_consumer_connection")
postgresql = manager.get_data_source("postgresql_connection")
```

## Features

### Data Sources
- **S3 DataSource**: Upload, download, list and manage files in S3 buckets
- **PostgreSQL DataSource**: Execute queries, manage transactions, and handle data. See [OLTP.README.md](OLTP.README.md) for details
- **Kafka Consumer DataSource**: Consume messages from Kafka topics
- **Kafka Producer DataSource**: Produce messages to Kafka topics

### Connectors
- **S3 Sink Connector**: Stream data from Kafka to S3, with support for Parquet file format


## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.