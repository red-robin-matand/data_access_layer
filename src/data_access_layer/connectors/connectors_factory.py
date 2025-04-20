from data_access_layer.connectors import Connector


class ConnectorsFactory:
    _registered_connectors = {}

    @classmethod
    def register_connector(cls, connector_type: str, connector_cls: Connector) -> None:
        cls._registered_connectors[connector_type] = connector_cls

    @classmethod
    def create_connector(cls, connector_type: str, name: str, kafka_consumer_datasource_name: str, data_sink_name: str) -> Connector:

        connector_cls = cls._registered_connectors.get(connector_type)

        if not connector_cls:
            raise ValueError(
                f"No connector registered for type: {connector_type}")

        return connector_cls(
            name=name,
            kafka_consumer_datasource_name=kafka_consumer_datasource_name,
            data_sink_name=data_sink_name
        )
