from abc import ABC


from data_access_layer.connections import Connection


class StreamConnection(Connection, ABC):

    def __init__(self, name: str, broker: str, topic: str) -> None:
        super().__init__(name)
        self._broker = broker
        self._topic = topic

    @property
    def broker(self) -> str:
        return self._broker

    @property
    def topic(self) -> str:
        return self._topic
