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

class StreamDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)

class KafkaDatasourceError(StreamDatasourceError):

    def __init__(self, message):
        
        super().__init__(message)

class KafkaProducerDatasourceError(KafkaDatasourceError):

    def __init__(self, message):
        
        super().__init__(message)

class MessageDeliveryError(KafkaProducerDatasourceError):

    def __init__(self, message):
        
        super().__init__(message)

class KafkaConsumerDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)

class CloudWatchDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)

class DataLakeDatasourceError(DatasourceException):

    def __init__(self, message):
        
        super().__init__(message)