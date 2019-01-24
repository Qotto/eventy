from typing import Type, Callable
from ..event.base import BaseEvent

__all__ = [
    'BaseEventConsumer'
]


class BaseEventConsumer:
    async def start(self):
        raise NotImplementedError

    def set_checkpoint_callback(self, checkpoint_callback):
        raise NotImplementedError
