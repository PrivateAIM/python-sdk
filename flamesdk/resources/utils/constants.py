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
