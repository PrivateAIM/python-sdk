"""Shared constants and enumerations used across the SDK."""

from enum import Enum


#: Prefix applied to the local storage tags that hold checkpoint saves.
CHECKPOINT_TAG_PREFIX = "checkpoint-"


#: Number of attempts a sidecar request is retried before it is given up on.
MAX_REQUEST_REPEATS = 5


class AnalysisStatus(Enum):
    """Lifecycle states an analysis node reports to the PO service.

    The node moves from ``STARTING`` through ``EXECUTING`` to a terminal state.
    ``STUCK`` and ``FAILED`` signal that the platform should intervene.
    """

    STARTING = "starting"
    STARTED = "started"

    STUCK = "stuck"

    STOPPING = "stopping"
    STOPPED = "stopped"

    EXECUTING = "executing"
    EXECUTED = "executed"
    FAILED = "failed"


class LogTypeLiteral(Enum):
    """Log severities accepted by :class:`~flamesdk.resources.utils.logging.FlameLogger`.

    Each member carries the wire value used when submitting the log (``.value``)
    together with the numeric severity of the matching ``logging`` level
    (``.level``), so a single member serves both purposes.
    """

    DEBUG = "debug", 10  # method=debug
    INFO = "info", 20  # method=info
    NOTICE = "notice", 25  # method=notice
    WARNING = "warn", 30  # method=warning
    ALERT = "alert", 33  # method=alert
    EMERGENCY = "emerg", 36  # method=emerg
    ERROR = "error", 40  # method=error
    CRITICAL = "crit", 50  # method=critical

    def __new__(cls, value: str, level: int):
        """Build a member from its wire value and its numeric severity."""
        obj = object.__new__(cls)
        obj._value_ = value  # keeps `.value` as the string
        obj.level = level
        return obj
