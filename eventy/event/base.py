# coding: utf-8
# Copyright (c) Qotto, 2018-2020

from typing import Any, Dict

from ..app.base import BaseApp
from ..utils import current_timestamp

__all__ = [
    'BaseEvent'
]


class BaseEvent:
    def __init__(self, name: str, data: Dict[str, Any]) -> None:
        if data is None:
            data = dict()

        if 'schema_version' not in data:
            data['schema_version'] = '0.0.0'

        timestamp = current_timestamp()
        if 'event_timestamp' in data:
            timestamp = data['event_timestamp']

        if 'timestamp' in data:
            timestamp = data['timestamp']

        data['timestamp'] = timestamp

        self.data = data
        self.name = name

    async def handle(self, app: BaseApp, corr_id: str):
        pass

    @property
    def context(self):
        return self.data['context']

    @property
    def schema_version(self):
        return self.data['schema_version']

    @property
    def correlation_id(self):
        return self.data['correlation_id']

    @property
    def timestamp(self):
        return self.data['timestamp']

    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        raise NotImplementedError
