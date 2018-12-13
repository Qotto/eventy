from ..service.command import BaseCommand
from ..event.base import BaseEvent

__all__ = [
    'BaseEventEmitter',
    'BaseCommandEmitter'
]


class BaseEventEmitter:
    async def send(self, event: BaseEvent, destination: str):
        raise NotImplementedError


class BaseCommandEmitter:

    async def send(self, command: BaseCommand, destination: str):
        raise NotImplementedError
