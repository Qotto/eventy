# coding: utf-8
# Copyright (c) Qotto, 2018

from base64 import b64encode
from datetime import datetime
from datetime import timezone
from secrets import token_urlsafe
from typing import Any
from pydoc import locate

__all__ = [
    'date2timestamp',
    'current_timestamp',
    'gen_correlation_id',
]


def date2timestamp(date: datetime) -> int:
    """
    Converts a UTC date to a UNIX timestamp, in milliseconds.

    >>> date2timestamp(datetime(1970, 1, 1, tzinfo=timezone.utc))
    0
    >>> date2timestamp(datetime(1970, 1, 2, tzinfo=timezone.utc))
    86400000
    """
    return round(date.timestamp()*1000)


def timestamp2date(timestamp: int) -> datetime:
    """
    Converts a UNIX timestamp in milliseconds to a UTC date.

    >>> timestamp2date(0)
    datetime.datetime(1970, 1, 1, 1, 0)
    >>> timestamp2date(24*3600*1000)
    datetime.datetime(1970, 1, 2, 1, 0)
    """
    return datetime.fromtimestamp(timestamp/1000.0)


def current_timestamp() -> int:
    """
    Returns current UNIX timestamp, in milliseconds.

    Example: checks that time flows in the right direction:

    >>> t1 = current_timestamp()
    >>> t2 = current_timestamp()
    >>> t1 <= t2
    True
    """
    return date2timestamp(datetime.now(timezone.utc))


def gen_correlation_id(prefix: str = None) -> str:
    """
    Generates a correlation ID that looks like `prefix:date:random`, where:

    - `prefix` is a fixed part that you can specify (any length)
    - `date` only depends on the current date (8 characters)
    - `random` is a random part  (4 characters)

    `date` and `random` are encoded as `[a-zA-Z0-9_-]`.
    """
    CORRELATION_ID_PREFIX_LENGTH = 6
    CORRELATION_ID_TOKEN_LENGTH = 3

    def ts2000res65536() -> bytes:
        """
        Converts current date to 6 bytes
        """
        ts_now = datetime.now(timezone.utc).timestamp()
        ts_2k = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
        return int(65536 * (ts_now - ts_2k)).to_bytes(6, 'big')

    if prefix is None:
        prefix = token_urlsafe(CORRELATION_ID_PREFIX_LENGTH)
    date = b64encode(ts2000res65536(), b'_-').decode('ascii')
    random = token_urlsafe(CORRELATION_ID_TOKEN_LENGTH)
    return f'{prefix}:{date}:{random}'


def get_dynamic_instance(full_class_name: str):
    """
    Returns an instance of the specified class

    >>> instance = get_dynamic_instance('eventy.consumer.base.BaseEventConsumer')
    >>> type(instance)
    <class 'eventy.consumer.base.BaseEventConsumer'>
    """
    class_ = load_class(full_class_name)
    return class_()


def load_class(full_class_name: str):
    """
    Loads the specified class

    >>> load_class('eventy.consumer.base.BaseEventConsumer')
    <class 'eventy.consumer.base.BaseEventConsumer'>
    """
    return locate(full_class_name)
