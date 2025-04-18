from data_access_layer.exceptions.dal_exception import DalException

class DatasourceException(DalException):

    def __init__(self, message):
        
        super().__init__(message)

class ObjectStoreDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)

class OLTPDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)