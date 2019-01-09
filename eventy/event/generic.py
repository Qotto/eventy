# coding: utf-8
# Copyright (c) Qotto, 2018

from .base import BaseEvent

from typing import Any, Dict

__all__ = [
    'GenericEvent'
]


class GenericEvent(BaseEvent):
    """
    Allows to create generic named events.

    >>> evt = GenericEvent(data={'quantity': '1L'})
    >>> evt.name()
    'GenericEvent'
    >>> evt.data['quantity']
    '1L'
    """

    def __init__(self, data: Dict[str, Any]) -> None:
        super().__init__(data=data)

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        event = cls(data=event_data)
        return event

    @classmethod
    def name(cls):
        return cls.__name__
