from ..service.service import Service
from typing import Type
from ..service.command import BaseCommand
from ..event.handler import EventHandler
from ..event.base import BaseEvent

__all__ = [
    'BaseEventConsumer',
    'BaseCommandConsumer'
]


class BaseEventConsumer:
    async def start(self):
        raise NotImplementedError

    def register_event_handler(self, event_handler: EventHandler, event_class: Type[BaseEvent]):
        raise NotImplementedError


class BaseCommandConsumer:

    async def start(self):
        raise NotImplementedError

    def register_service(self, service: Service, command_class: Type[BaseCommand]):
        raise NotImplementedError
