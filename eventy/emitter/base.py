from ..command.base import BaseCommand
from ..event.base import BaseEvent

__all__ = [
    'BaseEventEmitter',
    'BaseCommandEmitter'
]


class BaseEventEmitter:
    def __init__(self, *args, **kw) -> None:
        pass

    async def send(self, event: BaseEvent, destination: str):
        raise NotImplementedError


class BaseCommandEmitter:
    def __init__(self, *args, **kw) -> None:
        pass

    async def send(self, command: BaseCommand, destination: str):
        raise NotImplementedError
