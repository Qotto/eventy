# coding: utf-8
# Copyright (c) Qotto, 2018

from ..utils import current_timestamp, gen_correlation_id
from ..context import Context
from typing import Any, Dict

__all__ = [
    'BaseEvent'
]


class BaseEvent:
    def __init__(self, data: Dict[str, Any]) -> None:
        if data is None:
            data = dict()

        if 'correlation_id' not in data:
            data['correlation_id'] = gen_correlation_id()

        if 'schema_version' not in data:
            data['schema_version'] = '0.0.0'

        if 'event_timestamp' not in data:
            data['event_timestamp'] = current_timestamp()

        self.name = self.__class__.__name__
        self.data = data

    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        raise NotImplementedError

    async def handle(self, context: Context, corr_id: str):
        pass
