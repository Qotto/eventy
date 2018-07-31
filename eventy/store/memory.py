# coding: utf-8
# Copyright (c) Qotto, 2018

from ..event.base import BaseEvent
from ..serializer.base import BaseEventSerializer
from .base import BaseEventStore
from collections import deque

from typing import Deque

__all__ = [
    'MemoryEventStore',
]


class MemoryEventStore(BaseEventStore):
    """
    An ephemeral event store that uses memory as a storage.
    """
    def __init__(self, serializer: BaseEventSerializer) -> None:
        super().__init__(serializer)
        self._events: Deque[BaseEvent] = deque()
        self._pointer = 0

    def read(self) -> BaseEvent:
        try:
            serialized_event = self._events[self._pointer]
            decoded_event = self.serializer.decode(serialized_event)
            self._pointer += 1
            return decoded_event
        except IndexError:
            raise IndexError("No more events to read")

    def write(self, event: BaseEvent, *args, **kwargs) -> None:
        serialized_event = self.serializer.encode(event)
        self._events.append(serialized_event)

    def __next__(self) -> BaseEvent:
        if self._pointer >= len(self._events):
            raise StopIteration()
        return super().__next__()
