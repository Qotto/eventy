# coding: utf-8
# Copyright (c) Qotto, 2018

from .base import BaseEvent

from typing import Any, Dict

__all__ = [
    'GenericEvent',
]


class GenericEvent(BaseEvent):
    """
    Allows to create generic named events.

    >>> evt = GenericEvent("MilkBought", data={'quantity': '1L'})
    >>> evt.name
    'MilkBought'
    >>> evt.data['quantity']
    '1L'
    """
    def __init__(self, name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.name = name

    @classmethod
    def from_data(cls, event_name: str, event_data: Dict[str, Any]):
        event = cls(event_name, data=event_data)
        return event
