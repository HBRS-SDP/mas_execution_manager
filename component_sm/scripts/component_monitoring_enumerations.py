from enum import Enum


class Response(Enum):
    OKAY = 200
    STARTED = 201
    STOPPED = 202
    FAILED = 400
    INCOMPLETE = 401
    NOT_FOUND = 404


class MessageType(Enum):
    START = 'START'
    STOP = 'STOP'
    UPDATE = 'UPDATE'
