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


def flame_log(msg: Union[str, bytes],
              silent: bool,
              sep: str = ' ',
              end: str = '\n',
              file = None,
              flush: bool = False,
              log_type: str = 'normal',
              suppress_head: bool = False,
              suppress_tail: bool = False) -> None:
    """
    Print logs to console, if silent is set to False. May raise IOError, if suppress_head=False and log_type receives
    an invalid value.
    :param msg:
    :param silent:
    :param sep:
    :param end:
    :param file:
    :param flush:
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
            flame_log(f"When attempting to use logging function, this error occurred: {e}",
                      False,
                      log_type='error')

    if not silent:
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
        print(f"{head}{msg_cleaned}{tail}", sep=sep, end=end, file=file, flush=flush)


def declare_log_types(new_log_types: dict[str, str], silent: bool) -> None:
    """
    Declare new log_types to be added to log_type literals, and how/as what they should be interpreted by Flame
    (the latter have to be known values from HUB_LOG_LITERALS for existing log status fields).
    :param new_log_types:
    :param silent:
    :return:
    """
    for k, v in new_log_types.items():
        if v in [e.value for e in HUB_LOG_LITERALS]:
            if k not in _LOG_TYPE_LITERALS.keys():
                _LOG_TYPE_LITERALS[k] = v
                flame_log(f"Successfully declared new log_type={k} with Hub literal '{v}'.",
                          silent,
                          log_type='info')
            else:
                flame_log(f"Attempting to declare new log_type failed since log_type={k} "
                          f"already exists and cannot be overwritten.", silent, log_type='warning')
        else:
            flame_log(f"Attempting to declare new log_type failed. Attempted to declare new log_type for "
                      f"invalid Hub log field = {v} (known field values: {[e.value for e in HUB_LOG_LITERALS]}).",
                      False,
                      log_type='error')
