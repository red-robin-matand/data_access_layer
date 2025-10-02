import time
import pyarrow as pa

class MessageBuffer:
    def __init__(self, schema: pa.schema, max_messages: int = 1000, max_seconds: int = 10) -> None:
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        self.schema = schema
        self._buffer = []
        self._last_flush = time.time()

    def add(self, record: dict) -> None:
        self._buffer.append(record)

    def should_flush(self) -> bool:

        count_condition = len(self._buffer) >= self.max_messages
        if count_condition:
            return True

        time_condition = (time.time() - self._last_flush) >= self.max_seconds
        if time_condition:
            return True

        return False

    def flush(self) -> list:
        data = self._buffer
        self._buffer = []
        self._last_flush = time.time()
        return data

    def flush_as_arrow_table(self) -> pa.Table:
        table = pa.Table.from_pylist(self._buffer, schema=self.schema)
        self._buffer = []
        self._last_flush = time.time()
        return table

    def __len__(self):
        return len(self._buffer)
