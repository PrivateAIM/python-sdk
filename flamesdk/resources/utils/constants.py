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
    DEBUG = 'debug'         # method=debug,     level=10
    INFO = 'info'           # method=info,      level=20
    NOTICE = 'notice'       # method=notice,    level=25
    WARNING = 'warn'        # method=warning,   level=30
    ALERT = 'alert'         # method=alert,     level=33
    EMERGENCY = 'emerg'     # method=emerg,     level=36
    ERROR = 'error'         # method=error,     level=40
    CRITICAL = 'crit'       # method=critical,  level=50
