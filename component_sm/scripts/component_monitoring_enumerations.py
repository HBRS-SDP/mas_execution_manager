from enum import Enum


class Command(Enum):
    START = 'activate'
    SHUTDOWN = 'shutdown'
    STORE = 'store'

class ResponseCode(Enum):
    SUCCESS = 200
    FAILURE = 400

class MessageType(Enum):
    RESPONSE = 'response'
    REQUEST = 'request'
    INFO = 'info'
