from data_access_layer.exceptions.dal_exception import DalException


class ConnectorException(DalException):

    def __init__(self, message):
        
        super().__init__(message)