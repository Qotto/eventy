from typing import Any
from sanic import Sanic, Blueprint
from sanic.response import json
import logging
from .utils import load_class
import asyncio

logger = logging.getLogger(__name__)


class RuntimeContext:
    def __init__(self):
        self.serializer_class = None
        self.serializer = None
        self.serializer_event_classes = []

        self.command_consumer_class = None
        self.command_consumer = None
        self.command_handlers = []

        self.command_emitter_class = None
        self.command_emitter = None

        self.event_emitter_class = None
        self.event_emitter = None

        self.event_consumer_class = None
        self.event_consumer = None
        self.event_handlers = []

        self.http_handler = None
        self.http_handler_port = 8000

    def init_from_settings(self, settings: Any):

        self.init_event_serializer(settings)
        self.init_command_consumer(settings)
        self.init_command_emitter(settings)
        self.init_event_emitter(settings)
        self.init_event_consumer(settings)
        self.init_http_handler(settings)

    def start(self):
        asyncio.ensure_future(self.http_handler.create_server(
            host="0.0.0.0", port=self.http_handler_port))
        asyncio.get_event_loop().run_forever()

    def init_event_serializer(self, settings):
        if hasattr(settings, 'EVENTY_SERIALIZER'):
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

    def init_command_consummer(self, settings):
        if hasattr(settings, 'EVENTY_COMMAND_CONSUMMER'):
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

            asyncio.ensure_future(self.command_consumer.start())

    def init_command_emitter(self, settings):
        if hasattr(settings, 'EVENTY_COMMAND_EMITTER'):
            logger.debug(
                f"Initializing command emitter {settings.EVENTY_COMMAND_EMITTER}")
            self.command_emitter_class = load_class(
                settings.EVENTY_COMMAND_EMITTER)
            self.command_emitter = self.command_emitter_class(
                settings=settings)

    def init_event_emitter(self, settings):
        if hasattr(settings, 'EVENTY_EVENT_EMITTER'):
            logger.debug(
                f"Initializing event emitter {settings.EVENTY_EVENT_EMITTER}")
            self.event_emitter_class = load_class(
                settings.EVENTY_EVENT_EMITTER)
            self.event_emitter = self.event_emitter_class(settings=settings)

    def init_event_consummer(self, settings):
        if hasattr(settings, 'EVENTY_EVENT_CONSUMER'):
            logger.debug(
                f"Initializing event consummer {settings.EVENTY_EVENT_CONSUMER}")
            self.event_consumer_class = load_class(
                settings.EVENTY_EVENT_CONSUMER)
            self.event_consumer = self.event_consumer_class(settings=settings)
            if len(self.event_handlers) != 0:
                for event_handler in self.event_handlers:
                    self.event_consumer.register_event_handler(
                        event_handler=event_handler['event_handler'], event_class=event_handler['event_class'])

            asyncio.ensure_future(self.event_consumer.start())

    def init_http_handler(self, settings):
        self.http_handler = Sanic()
        self.http_handler.blueprint(health_bp)

        if hasattr(settings, 'EVENTY_HTTP_HANDLER_PORT'):
            self.http_handler_port = settings.EVENTY_HTTP_HANDLER_PORT

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


health_bp = Blueprint('health')


@health_bp.route('/health')
async def health(request):
    logger.debug('Health check called')
    return json({}, status=200)
