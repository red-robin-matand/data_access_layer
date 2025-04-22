from data_access_layer.exceptions.dal_exception import DalException


class ConnectorException(DalException):

    def __init__(self, message):

        super().__init__(message)


class UnknownConnectorType(ConnectorException):
    def __init__(self, message: str):
        super().__init__(message)


class MissingConfigurationKey(ConnectorException):
    def __init__(self, message: str):
        super().__init__(message)
