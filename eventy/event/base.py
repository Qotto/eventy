# coding: utf-8
# Copyright (c) Qotto, 2018

from ..utils import current_timestamp, gen_correlation_id
from typing import Any, Dict
from ..app.base import BaseApp

__all__ = [
    'BaseEvent'
]


class BaseEvent:
    def __init__(self, name: str, data: Dict[str, Any]) -> None:
        if data is None:
            data = dict()

        if 'schema_version' not in data:
            data['schema_version'] = '0.0.0'

        if 'timestamp' not in data:
            data['timestamp'] = current_timestamp()

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
