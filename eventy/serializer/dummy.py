# coding: utf-8
# Copyright (c) Qotto, 2018

from ..event.base import BaseEvent
from .base import BaseEventSerializer

__all__ = [
    'DummyEventSerializer',
]


class DummyEventSerializer(BaseEventSerializer):
    def encode(self, event: BaseEvent) -> BaseEvent:
        return event

    def decode(self, event: BaseEvent) -> BaseEvent:
        return event
