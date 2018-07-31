# coding: utf-8
# Copyright (c) Qotto, 2018

from ..event.base import BaseEvent
from ..serializer.base import BaseEventSerializer

__all__ = [
    'BaseEventStore',
]


class BaseEventStore:
    def __init__(self, serializer: BaseEventSerializer) -> None:
        self.serializer = serializer

    def read(self) -> BaseEvent:
        raise NotImplementedError

    def write(self, event: BaseEvent) -> None:
        raise NotImplementedError

    def __iter__(self) -> 'BaseEventStore':
        return self

    def __next__(self) -> BaseEvent:
        return self.read()
