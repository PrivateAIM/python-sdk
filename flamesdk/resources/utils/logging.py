import string
import time
from enum import Enum
from typing import Union
import queue
import logging
import json
import sys
import threading
from collections.abc import Iterable

from flamesdk.resources.utils.constants import AnalysisStatus


_LOG_TYPE_LITERALS = ['debug',      # method=debug,     level=10
                      'info',       # method=info,      level=20
                      'notice',     # method=notice,    level=25
                      'warn',       # method=warning,   level=30
                      'alert',      # method=alert,     level=33
                      'emerg',      # method=emerg,     level=36
                      'error',      # method=error,     level=40
                      'crit']       # method=critical,  level=50


class FlameLogger:
    def __init__(self, silent: bool = False) -> None:
        """
        Initialize the FlameLog class with a silent mode.
        :param silent: If True, logs will not be printed to console.
        """
        self.queue = queue.Queue()
        self.po_api = None  # Placeholder for PO_API instance
        self.silent = silent
        self.runstatus = AnalysisStatus.STARTING.value  # Default status for logs
        self.log_ph = ""
        self.progress = 0
        self.logger = _get_logger()

    def add_po_api(self, po_api) -> None:
        """
        Add a POAPI instance to the FlameLogger.
        :param po_api: An instance of POAPI.
        """
        self.po_api = po_api

    def set_runstatus(self, status: str) -> None:
        """
        Set the run status for the logger.
        :param status: The status to set (e.g., 'starting', 'executing', 'stopped', 'executed', 'failed').
        """
        if status not in [s.value for s in AnalysisStatus]:
            status = AnalysisStatus.FAILED.value
        if status == AnalysisStatus.STOPPED.value:
            self.new_log(msg='Analysis execution was stopped on another node.', log_type='info')
        self.runstatus = status

    def set_progress(self, progress: Union[int, float]) -> None:
        """
        Set the analysis progress in the logger.
        :param progress:
        """
        if isinstance(progress, float):
            progress = int(progress)
        if not (0 <= progress <= 100):
            self.new_log(msg=f"Invalid progress: {progress} (should be a numeric value between 0 and 100).",
                         log_type='warn')
        elif self.progress > progress:
            self.new_log(msg=f"Progress value needs to be higher to current progress (i.e. only register progress, "
                             f"if actual progress has been made).",
                         log_type='warn')
        else:
            self.progress = progress

    def send_logs_from_queue(self) -> None:
        """
        Send all logs from the queue to the POAPI.
        """
        if self.po_api is None:
            try:
                raise ValueError("POAPI instance is not set. Use add_po_api() to set it.")
            except ValueError as e:
                self.raise_error(repr(e))
        if not self.queue.empty():
            while not self.queue.empty():
                log_dict = self.queue.get()
                self.po_api.stream_logs(log_dict['msg'], log_dict['log_type'], log_dict['status'], log_dict['progress'])
                self.queue.task_done()

    def new_log(self,
                msg: Union[str, bytes, Iterable],
                sep: str = '',
                end: str = '',
                log_type: str = 'info',
                append: bool = False,
                halt_submission: bool = False) -> None:
        """
        Print logs to console, if silent is set to False. May raise IOError, if append=False and log_type receives
        an invalid value.
        :param msg:
        :param sep:
        :param end:
        :param log_type:
        :param append:
        :param halt_submission:
        :return:
        """
        if log_type not in _LOG_TYPE_LITERALS:
            try:
                raise IOError(f"Invalid log type given to logging function "
                              f"(known log_types={_LOG_TYPE_LITERALS}, received log_type={log_type}).")
            except IOError as e:
                self.raise_error(f"When attempting to use logging function, this error occurred: {repr(e)}")

        if not self.silent:
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8', errors='replace')
                log = ''.join(filter(lambda x: x in string.printable, msg)) + end
            elif isinstance(msg, str):
                log_type = msg + end
            elif isinstance(msg, Iterable):
                log = sep.join(msg) + end
            else:
                self.raise_error(f"Attempted to log msg of neither type str, bytes, or joinable iterable "
                                 f"(type(msg)={type(msg)}).")

            if log_type == 'debug':
                self.logger.debug(log)
            elif log_type == 'info':
                self.logger.info(log)
            elif log_type == 'notice':
                self.logger.notice(log)
            elif log_type == 'warn':
                self.logger.warning(log)
            elif log_type == 'alert':
                self.logger.alert(log)
            elif log_type == 'emerg':
                self.logger.emerg(log)
            elif log_type == 'error':
                self.logger.error(log)
            elif log_type == 'crit':
                self.logger.critical(log)
            else:
                pass # Impossible to reach

            if halt_submission:
                self.log_ph = log
            else:
                if append:
                    log = self.log_ph + log
                    self.log_ph = ""
                self._submit_logs(log, log_type, self.runstatus)
        
    def raise_error(self, message: str, seconds: int = 100) -> None:
        self.set_runstatus(AnalysisStatus.FAILED.value)
        self.new_log(message, log_type="error")
        time.sleep(seconds)

    def _submit_logs(self, log: str, log_type: str, status: str) -> None:
        if self.po_api is None:
            log_dict = {
                "msg": log,
                "log_type": log_type,
                "status": status,
                "progress": self.progress
            }
            self.queue.put(log_dict)
        else:
            try:
                self.send_logs_from_queue()
                self.po_api.stream_logs(log, log_type, status, self.progress)
            except Exception as e:
                # If sending fails, we can still queue the log
                log_dict = {
                    "msg": log,
                    "log_type": log_type,
                    "status": status,
                    "progress": self.progress
                }
                self.queue.put(log_dict)

                # But also create new error log for queue
                error_log_dict = {
                    "msg": f"Failed to send log to POAPI: {repr(e)}",
                    "log_type": 'warning',
                    "status": status,
                    "progress": self.progress
                }
                self.queue.put(error_log_dict)


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line for structured log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize the log record as a single JSON object on one line.

        Always includes ``timestamp``, ``level``, ``logger``, ``module``, and
        ``msg`` fields. When the record carries exception info, a formatted
        traceback is added under ``error``.
        """
        log = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "msg": record.getMessage(),
        }

        if record.exc_info:
            log["error"] = self.formatException(record.exc_info)

        return json.dumps(log, default=str)  # for non-serializable msgs


def _get_logger() -> logging.Logger:
    """Return a process-wide logger configured for JSON output.

    Returns:
        A :class:`logging.Logger` ready for use.
    """
    _set_custom_log_level(25, 'NOTICE')
    _set_custom_log_level(33, 'ALERT')
    _set_custom_log_level(36, 'EMERG')

    root = logging.getLogger()
    if not any(isinstance(h.formatter, JsonFormatter) for h in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        root.addHandler(handler)
        root.setLevel(logging.INFO)

    sys.excepthook = _log_uncaught
    threading.excepthook = lambda a: _log_uncaught(
        a.exc_type, a.exc_value, a.exc_traceback
    )

    logger = logging.getLogger(__name__)
    return logger


def _set_custom_log_level(level, level_name):
    """Register a new log level and expose it as a method on ``Logger`` and module function.

    After calling ``_set_custom_log_level(21, 'ACTION')`` you can write
    ``logger.action("...")`` and ``logging.action("...")``.

    Args:
        level: Integer log level (between existing stdlib levels).
        level_name: Human-readable name; used uppercase as the level name and
            lowercase as the method/function name.
    """
    def logForLevel(self, message, *args, **kws):
        if self.isEnabledFor(level):
            self._log(level, message, args, **kws)

    def logToRoot(message, *args, **kwargs):
        logging.log(level, message, *args, **kwargs)

    logging.addLevelName(level, level_name.upper())
    setattr(logging, level_name.upper(), level)
    setattr(logging.getLoggerClass(), level_name.lower(), logForLevel)
    setattr(logging, level_name.lower(), logToRoot)


def _log_uncaught(exc_type, exc, tb):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc, tb)
        return
    logging.getLogger("uncaught").critical(
        "Unhandled exception", exc_info=(exc_type, exc, tb)
    )
