from sqlalchemy import create_engine

from data_access_layer.connections import Connection
from data_access_layer.connections import OLTPConnection


class PostgreSQLConnection(OLTPConnection):
    

    class PostgreSQLConfigKeys(Connection.ConfigKeys):
        NAME = 'name'
        HOST = 'host'
        PORT = 'port'
        DATABASE = 'database'
        USERNAME = 'username'
        PASSWORD = 'password'
        SSL = 'ssl'
        SSL_KEY = 'key'
        SSL_CERT = 'cert'
        SSL_CA = 'ca'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if
                    member.value not in [cls.SSL.value, cls.SSL_KEY.value, cls.SSL_CERT.value, cls.SSL_CA.value]]

    def __init__(self, name: str, host: str, port: int, database: str, username: str, password: str,
                 ssl_keyfile_path: str = None, ssl_certfile_path: str = None, ssl_ca_certs: str = None):

        super().__init__(
            name=name,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            ssl_keyfile_path=ssl_keyfile_path,
            ssl_certfile_path=ssl_certfile_path,
            ssl_ca_certs=ssl_ca_certs,
        )

    @classmethod
    def from_dict(cls, config: str):
        
        required_config_keys = cls.PostgreSQLConfigKeys.required_keys()
        cls.validate_dict_keys(config, required_config_keys)

        return cls(
            config[cls.PostgreSQLConfigKeys.NAME.value],
            config[cls.PostgreSQLConfigKeys.HOST.value],
            config[cls.PostgreSQLConfigKeys.PORT.value],
            config[cls.PostgreSQLConfigKeys.DATABASE.value],
            config[cls.PostgreSQLConfigKeys.USERNAME.value],
            config[cls.PostgreSQLConfigKeys.PASSWORD.value],
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(
                cls.PostgreSQLConfigKeys.SSL.SSL_KEY.value),
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(
                cls.PostgreSQLConfigKeys.SSL.SSL_CERT.value),
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(
                cls.PostgreSQLConfigKeys.SSL.SSL_CA.value)
        )

    def _create_engine(self) -> None:

        ssl_args = {}
        if self._ssl:
            ssl_args = {
                'sslmode': 'require',  # PostgreSQL SSL mode
                'sslcert': self._ssl_certfile_path,
                'sslkey': self._ssl_keyfile_path,
                'sslrootcert': self._ssl_ca_certs,
            }

        self._connection_engine = create_engine(
            self.create_connection_string(), connect_args=ssl_args)

    def create_connection_string(self) -> str:
        
        return f"postgresql+psycopg2://{self._username}:{self._password}@{self._host}:{self._port}/{self._database}"

    def disconnect(self):
        
        if self._connection_engine:
            self._connection_engine.dispose()