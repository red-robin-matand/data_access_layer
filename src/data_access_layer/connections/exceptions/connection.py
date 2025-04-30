from data_access_layer.exceptions.dal_exception import DalException


class ConnectionException(DalException):

    def __init__(self, message):
        
        super().__init__(message)


class UnknownConnectionType(ConnectionException):

    def __init__(self, message):
        
        super().__init__(message)


class MissingConfigurationKey(ConnectionException):

    def __init__(self, message):
        
        super().__init__(message)

class DisconnectFailed(ConnectionException):

    def __init__(self, message):
        
        super().__init__(message)

class ConnectionFailed(ConnectionException):

    def __init__(self, message):
        
        super().__init__(message)
