import string
import time
from enum import Enum
from typing import Union


class HUB_LOG_LITERALS(Enum):
    info_log = 'info'
    notice_message = 'notice'
    debug_log = 'debug'
    warning_log = 'warn'
    alert_log = 'alert'
    emergency_log = 'emerg'
    error_code = 'error'
    critical_error_code = 'crit'


_LOG_TYPE_LITERALS = {'info': HUB_LOG_LITERALS.info_log.value,
                      'normal': HUB_LOG_LITERALS.info_log.value,
                      'notice': HUB_LOG_LITERALS.notice_message.value,
                      'debug': HUB_LOG_LITERALS.debug_log.value,
                      'warning': HUB_LOG_LITERALS.warning_log.value,
                      'alert': HUB_LOG_LITERALS.alert_log.value,
                      'emergency': HUB_LOG_LITERALS.emergency_log.value,
                      'error': HUB_LOG_LITERALS.error_code.value,
                      'critical-error': HUB_LOG_LITERALS.critical_error_code.value}

class FlameLogger:

    def __init__(self, silent: bool = False):
        """
        Initialize the FlameLog class with a silent mode.
        :param silent: If True, logs will not be printed to console.
        """
        self.queue = []
        self.po_client = None  # Placeholder for POClient instance
        self.silent = silent
        self.runstatus = 'starting'  # Default status for logs

    def add_po_client(self, po_client) -> None:
        """
        Add a POClient instance to the FlameLogger.
        :param po_client: An instance of POClient.
        """
        self.po_client = po_client

    def set_runstatus(self, status: str) -> None:
        """
        Set the run status for the logger.
        :param status: The status to set (e.g., 'running', 'completed', 'failed').
        """
        if status not in ['starting', 'running', 'finished', 'failed']:
            status = 'failed'  # Default to 'running' if an invalid status is provided
        self.runstatus = status

    def send_logs_from_queue(self) -> None:
        """
        Send all logs from the queue to the POClient.
        """
        if self.po_client is None:
            raise ValueError("POClient instance is not set. Use add_po_client() to set it.")

        for log in self.queue:
            self.po_client.stream_logs(log['msg'], log['log_type'], log['status'], self.silent)
        self.queue.clear()

    def new_log(self,
                msg: Union[str, bytes],
                sep: str = ' ',
                end: str = '\n',
                file = None,
                log_type: str = 'normal',
                suppress_head: bool = False,
                suppress_tail: bool = False) -> None:
        """
        Print logs to console, if silent is set to False. May raise IOError, if suppress_head=False and log_type receives
        an invalid value.
        :param msg:
        :param sep:
        :param end:
        :param file:
        :param log_type:
        :param suppress_head:
        :param suppress_tail:
        :return:
        """
        if log_type not in _LOG_TYPE_LITERALS.keys():
            try:
                raise IOError(f"Invalid log type given to logging function "
                              f"(known log_types={_LOG_TYPE_LITERALS.keys()}, received log_type={log_type}).")
            except IOError as e:
                self.new_log(f"When attempting to use logging function, this error occurred: {e}",
                             log_type='error')

        log = None
        if not self.silent:
            if isinstance(msg, bytes):
                msg = msg.decode('utf-8', errors='replace')
            msg_cleaned = ''.join(filter(lambda x: x in string.printable, msg))
            if suppress_head:                                           # suppressing head (ignore log_type)
                head = ''
            elif log_type == 'normal':                                  # if log_type=='normal', add nothing to head
                head = f"[flame {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] "
            else:                                                       # else, add uppercase log_type
                head = f"[flame -- {log_type.upper()} -- {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] "
            if suppress_tail:
                tail = ''
            else:
                tail = f"!suff!{_LOG_TYPE_LITERALS[log_type]}"
            log = f"{head}{msg_cleaned}{tail}"
            print(log, sep=sep, end=end, file=file)

        if log is not None:
            self._submit_logs(log, log_type, self.runstatus)

    def waiting_for_health_check(self, seconds: int = 100) -> None:
        time.sleep(seconds)

    def declare_log_types(self, new_log_types: dict[str, str]) -> None:
        """
        Declare new log_types to be added to log_type literals, and how/as what they should be interpreted by Flame
        (the latter have to be known values from HUB_LOG_LITERALS for existing log status fields).
        :param new_log_types:
        :return:
        """
        for k, v in new_log_types.items():
            if v in [e.value for e in HUB_LOG_LITERALS]:
                if k not in _LOG_TYPE_LITERALS.keys():
                    _LOG_TYPE_LITERALS[k] = v
                    self.new_log(f"Successfully declared new log_type={k} with Hub literal '{v}'.",
                                 log_type='info')
                else:
                    self.new_log(f"Attempting to declare new log_type failed since log_type={k} "
                              f"already exists and cannot be overwritten.", log_type='warning')
            else:
                self.new_log(f"Attempting to declare new log_type failed. Attempted to declare new log_type for "
                             f"invalid Hub log field = {v} (known field values: {[e.value for e in HUB_LOG_LITERALS]}).",
                             log_type='error')

    def _submit_logs(self, log,log_type, status):
        if self.po_client is None:
            log_dict = {
                "msg": log,
                "log_type":log_type,
                "status": status
            }
            self.queue.append(log_dict)
        else:
            self.po_client.stream_logs(log, log_type, status)

