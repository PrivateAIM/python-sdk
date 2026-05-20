from enum import Enum


class AnalysisStatus(Enum):
    STARTING = 'starting'
    STARTED = 'started'

    STUCK = 'stuck'

    STOPPING = 'stopping'
    STOPPED = 'stopped'

    EXECUTING = 'executing'
    EXECUTED = 'executed'
    FAILED = 'failed'


class LogTypeLiteral(Enum):
    DEBUG = 'debug', 10         # method=debug
    INFO = 'info', 20           # method=info
    NOTICE = 'notice', 25       # method=notice
    WARNING = 'warn', 30        # method=warning
    ALERT = 'alert', 33         # method=alert
    EMERGENCY = 'emerg', 36     # method=emerg
    ERROR = 'error', 40         # method=error
    CRITICAL = 'crit', 50       # method=critical

    def __new__(cls, value: str, level: int):
        obj = object.__new__(cls)
        obj._value_ = value  # keeps `.value` as the string
        obj.level = level
        return obj
