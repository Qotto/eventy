# coding: utf-8
# Copyright (c) Qotto, 2018

from ..utils import current_timestamp

from typing import Any, Dict

__all__ = [
    'BaseEvent',
]


class BaseEvent:
    def __init__(self, data: Dict[str, Any] = None, correlation_id: str = None) -> None:
        if data is None:
            data = dict()
        if 'event_timestamp' not in data:
            data['event_timestamp'] = current_timestamp()
        if correlation_id is not None:
            data['correlation_id'] = correlation_id
        self.name = self.__class__.__name__
        self.data = data

    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        raise NotImplementedError
