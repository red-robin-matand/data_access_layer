from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from data_access_layer.connections import KafkaConnection
from data_access_layer.connections.exceptions import ConnectionFailed


class KafkaConsumerConnection(KafkaConnection):

    class KafkaConfigKeys(KafkaConnection.ConfigKeys):
        NAME = 'name'
        BROKER = 'broker'
        TOPIC = 'topic'
        GROUP_ID = 'group_id'
        OFFSET = 'offset'
        SASL = 'sasl'
        SASL_USERNAME = 'sasl_username'
        SASL_PASSWORD = 'sasl_password'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if member.value not in [
                cls.SASL.value,
                cls.SASL_USERNAME.value,
                cls.SASL_PASSWORD.value,
            ]]

    def __init__(self, name: str, broker: str, topic: str, group_id: str, offset: str,
                 sasl_username: str = None, sasl_password: str = None) -> None:
        super().__init__(
            name=name,
            broker=broker,
            topic=topic,
        )
        self._group_id = group_id
        self._offset = offset
        self._connection_engine: Consumer = None
        self._sasl = all([sasl_username, sasl_password])
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password

    @classmethod
    def from_dict(cls, config: dict):
        config_keys = cls.KafkaConfigKeys.required_keys()
        cls.validate_dict_keys(config, config_keys)

        sasl_config = config.get(cls.KafkaConfigKeys.SASL.value, {})

        return cls(
            name=config[cls.KafkaConfigKeys.NAME.value],
            broker=config[cls.KafkaConfigKeys.BROKER.value],
            topic=config[cls.KafkaConfigKeys.TOPIC.value],
            group_id=config[cls.KafkaConfigKeys.GROUP_ID.value],
            offset=config[cls.KafkaConfigKeys.OFFSET.value],
            sasl_username=sasl_config.get(
                cls.KafkaConfigKeys.SASL_USERNAME.value, None),
            sasl_password=sasl_config.get(
                cls.KafkaConfigKeys.SASL_PASSWORD.value, None),
        )

    def connect(self):
        if not self._connection_engine:
            self._create_engine()

    def disconnect(self) -> None:
        if self._connection_engine:
            self._connection_engine.close()
            self._connection_engine = None

    def _create_engine(self) -> None:
        config = {
            'bootstrap.servers': self._broker,
            'group.id': self._group_id,
            'auto.offset.reset': 'earliest',
        }
        sasl_args = {}
        if self._sasl:
            sasl_args = {
                'sasl.username': self._sasl_username,
                'sasl.password': self._sasl_password,
                'sasl.mechanisms': 'PLAIN',
                'security.protocol': 'SASL_SSL',
            }
            config.update(sasl_args)
        self._connection_engine = Consumer(config)

        try:
            metadata = self._connection_engine.list_topics(self._topic)
            partitions = list(metadata.topics[self._topic].partitions.keys())

            if not partitions:
                raise ConnectionFailed(f"No partitions found for topic {self._topic}")

            topic_partitions = [TopicPartition(self._topic, p) for p in partitions]
            self._connection_engine.assign(topic_partitions)

            # if self._offset == 'earliest':
            #     self._connection_engine.seek_to_beginning(topic_partitions)

        except Exception as e:
            raise ConnectionFailed(f"Failed to assign partitions for topic {self._topic}: {e}") from e

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def offset(self) -> str:
        return self._offset
