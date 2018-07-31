# coding: utf-8
# Copyright (c) Qotto, 2018

from ..event.base import BaseEvent

from typing import Any

__all__ = [
    'BaseEventSerializer',
]


class BaseEventSerializer:
    def encode(self, event: BaseEvent) -> Any:
        raise NotImplementedError

    def decode(self, encoded_event: Any) -> BaseEvent:
        raise NotImplementedError
