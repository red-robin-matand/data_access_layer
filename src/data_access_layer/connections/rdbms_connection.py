from abc import ABC, abstractmethod

from sqlalchemy import exc, text, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from data_access_layer.connections import Connection


class RDBMSConnection(Connection, ABC):
    
    def __init__(self, name: str, host: str, port: int, database: str, username: str, password: str,
                 ssl_keyfile_path: str = None, ssl_certfile_path: str = None, ssl_ca_certs: str = None):
        super().__init__(name)
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssl = all([ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs])
        self._ssl_keyfile_path = ssl_keyfile_path
        self._ssl_certfile_path = ssl_certfile_path
        self._ssl_ca_certs = ssl_ca_certs
        self._database = database
        self._metadata = None
        self._session_maker = None
        self._automap_base_model = None
        self._declarative_base_model = None
        self._user_defined_models = []
        self._reflected_tables = set()
        self._initiate_declarative_base_model()

    @property
    def declarative_base_model(self):
        return self._declarative_base_model

    @property
    def automap_base_model(self):
        return self._automap_base_model

    def get_new_session(self):
        return self._session_maker()

    def connect(self) -> None:
        self._create_engine()
        self._session_maker = sessionmaker(bind=self._connection_engine)

        if self._metadata is None or self._automap_base_model is None:
            self._initiate_automap_base_model()

    def reflect_tables(self, table_names: list) -> None:
        
        tables_to_reflect = set(table_names) - self._reflected_tables

        if tables_to_reflect:
            self._metadata.reflect(
                bind=self._connection_engine, only=list(tables_to_reflect))
            self._reflected_tables.update(tables_to_reflect)
            self._automap_base_model.prepare(
                name_for_scalar_relationship=self._name_for_scalar_relationship)

    def _name_for_scalar_relationship(self, base, local_cls, referred_cls, constraint) -> str:
        return referred_cls.__name__.lower() + "_relation"

    def _initiate_automap_base_model(self) -> None:
        
        self._metadata = MetaData()
        self._automap_base_model = automap_base(metadata=self._metadata)

    def _initiate_declarative_base_model(self) -> None:
        
        self._declarative_base_model = declarative_base()

    def register_model(self, model) -> None:
        
        assert issubclass(
            model, self._declarative_base_model), "Model must be a subclass of Base."
        self._user_defined_models.append(model)

    def create_all_user_defined_models(self) -> None:
        
        self._declarative_base_model.metadata.create_all(
            self._connection_engine)

    def check_health(self) -> bool:
        try:
            session = self.get_new_session()
            session.execute(text("SELECT 1"))
            session.close()
            return True
        except exc.DBAPIError:
            return False
