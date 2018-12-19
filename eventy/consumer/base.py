from typing import Type, Callable
from ..command.base import BaseCommand
from ..event.base import BaseEvent

__all__ = [
    'BaseEventConsumer',
    'BaseCommandConsumer'
]


class BaseEventConsumer:
    async def start(self):
        raise NotImplementedError

    def register_event_handler(self, event_handler: Callable[[BaseEvent, str], None], event_class: Type[BaseEvent]):
        raise NotImplementedError


class BaseCommandConsumer:

    async def start(self):
        raise NotImplementedError

    def register_command_handler(self, command_handler: Callable[[BaseCommand, str], None], command_class: Type[BaseCommand]):
        raise NotImplementedError
