from typing import Any
import logging
from .utils import load_class


logger = logging.getLogger(__name__)


class RuntimeContext:
    def __init__(self):
        self.serializer_class = None
        self.serializer = None
        self.serializer_event_classes = []

        self.command_consumer_class = None
        self.command_consumer = None
        self.command_handlers = []

        self.event_emitter_class = None
        self.event_emitter = None

        self.event_consumer_class = None
        self.event_consumer = None
        self.event_handlers = []

    def init_from_settings(self, settings: Any):

        if settings.EVENTY_SERIALIZER:
            logger.debug(
                f"Initializing serializer {settings.EVENTY_SERIALIZER}")
            self.serializer_class = load_class(settings.EVENTY_SERIALIZER)
            self.serializer = self.serializer_class(settings=settings)
            if len(self.serializer_event_classes) != 0:
                for event_class in self.serializer_event_classes:
                    logger.debug(
                        f"Registering class {event_class['event_class']} for event {event_class['event_name']}")
                    self.serializer.register_event_class(
                        event_class=event_class['event_class'], event_name=event_class['event_name'])

        if settings.EVENTY_COMMAND_CONSUMMER:
            logger.debug(
                f"Initializing command consumer {settings.EVENTY_COMMAND_CONSUMMER}")
            self.command_consumer_class = load_class(
                settings.EVENTY_COMMAND_CONSUMMER)
            self.command_consumer = self.command_consumer_class(
                settings=settings)
            if len(self.command_handlers) != 0:
                for command_handler in self.command_handlers:
                    self.command_consumer.register_command_handler(
                        command_handler=command_handler['command_handler'], command_class=command_handler['command_class'])

        if settings.EVENTY_EVENT_EMITTER:
            self.event_emitter_class = load_class(
                settings.EVENTY_EVENT_EMITTER)
            self.event_emitter = self.event_emitter_class(settings=settings)

        if settings.EVENTY_EVENT_CONSUMER:
            self.event_consumer_class = load_class(
                settings.EVENTY_EVENT_CONSUMER)
            self.event_consumer = self.event_consumer_class(settings=settings)
            if len(self.event_handlers) != 0:
                for event_handler in self.event_handlers:
                    self.event_consumer.register_event_handler(
                        event_handler=event_handler['event_handler'], event_class=event_handler['event_class'])

    def register_event_class(self, event_class, event_name):
        if self.serializer:
            self.serializer.register_event_class(
                event_class=event_class, event_name=event_name)
        else:
            self.serializer_event_classes.append(
                {'event_class': event_class, 'event_name': event_name})

    def register_command_handler(self, command_handler, command_class):
        if self.command_consumer:
            self.command_consumer.register_command_handler(
                command_handler=command_handler, command_class=command_class)
        else:
            self.command_handlers.append(
                {'command_handler': command_handler, 'command_class': command_class})

    def register_event_handler(self, event_handler, event_class):
        if self.event_consumer:
            self.event_consumer.register_event_handler(
                event_handler=event_handler, event_class=event_class)
        else:
            self.event_handlers.append(
                {'event_handler': event_handler, 'event_class': event_class})


runtime_context = RuntimeContext()


def event_class(event_name):
    def decorator(event_class):
        runtime_context.register_event_class(event_class, event_name)
        return event_class
    return decorator


def command_class(command_name):
    return event_class(event_name=command_name)


def command_handler(command_class):
    def decorator(command_handler):
        runtime_context.register_command_handler(
            command_handler, command_class)
        return command_handler
    return decorator


def event_handler(event_class):
    def decorator(event_handler):
        runtime_context.register_event_handler(event_handler, event_class)
        return event_handler
    return decorator
