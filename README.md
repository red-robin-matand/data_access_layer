# Data Access Layer

A Python library that provides a unified interface for connecting to different data sources: object stores (S3), streaming platforms (Kafka), and relational databases. It uses a factory pattern to create connections based on YAML configuration.

## Installation

```sh
pip install data_access_layer
```

## Getting Started

1. Set up your connections configuration in a YAML file:

```yaml
connections:
  - type: "s3"
    name: "my-s3"
    access_key: "your-access-key"
    secret_key: "your-secret-key"
    region: "my-region"
    connections: 10
    bucket: "my-bucket"

  - type: "kafka_producer"
    name: "my-producer"
    broker: "localhost:9092"
    topic: "my-topic"

  - type: "kafka_consumer"
    name: "my-consumer"
    broker: "localhost:9092"
    topic: "my-topic"
    group_id: "my-group"
    offset: "earliest"
```

2. Set the environment variable for your config:

```sh
export CONNECTIONS_YAML=/path/to/your/connections.yaml
```


## Supported Connection Types

- `s3`: Amazon S3 connections using boto3
- `kafka_producer`: Kafka producer connections using confluent-kafka
- `kafka_consumer`: Kafka consumer connections using confluent-kafka


## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.