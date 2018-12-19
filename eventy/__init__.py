import logging
from .utils import load_class
import asyncio
from sanic import Sanic, Blueprint
from sanic.response import json
from typing import Type, Callable, Dict
from .command.base import BaseCommand
from .event.base import BaseEvent
from .context import Context

health_bp = Blueprint('health')


@health_bp.route('/health')
async def health(request):
    return json({}, status=200)


class Eventy:
    def __init__(self, settings: object):
        self.logger = logging.getLogger(__name__)
        self.context = Context()
        self.context.set('settings', settings)

        serializer = self.create_event_serializer(settings)
        self.context.set('serializer', serializer)

        self.context.set('command_consumer',
                         self.create_command_consumer(settings, serializer))

        self.context.set('command_emitter',
                         self.create_command_emitter(settings, serializer))

        self.context.set(
            'event_emitter', self.create_event_emitter(settings, serializer))

        self.context.set('event_consumer',
                         self.create_event_consumer(settings, serializer))

        self.http_handler = self.create_http_handler(settings)
        self.context.set(
            'http_handler', self.http_handler)

        self.register_http_blueprint(health_bp)

    def start(self):
        asyncio.ensure_future(self.http_handler.create_server(
            host="0.0.0.0", port=self.http_handler_port))
        asyncio.get_event_loop().run_forever()

    def create_event_serializer(self, settings):
        serializer = None
        if hasattr(settings, 'EVENTY_SERIALIZER'):
            self.logger.debug(
                f"Initializing serializer {settings.EVENTY_SERIALIZER}")
            serializer_class = load_class(settings.EVENTY_SERIALIZER)
            serializer = serializer_class(settings=settings)
        return serializer

    def create_command_consumer(self, settings, serializer):
        command_consumer = None
        if hasattr(settings, 'EVENTY_COMMAND_CONSUMER'):
            self.logger.debug(
                f"Initializing command consumer {settings.EVENTY_COMMAND_CONSUMER}")

            if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
                raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
            if not hasattr(settings, 'EVENTY_COMMAND_CONSUMER_SOURCE'):
                raise Exception(
                    "Missing EVENTY_COMMAND_CONSUMER_SOURCE config")
            event_topic = settings.EVENTY_COMMAND_CONSUMER_SOURCE
            event_group = settings.EVENTY_COMMAND_CONSUMER_SOURCE.split(
                '-')[0]

            command_consumer_class = load_class(
                settings.EVENTY_COMMAND_CONSUMER)
            command_consumer = command_consumer_class(
                context=self.context, serializer=serializer, bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER, username=settings.KAFKA_USERNAME, password=settings.KAFKA_PASSWORD, event_topic=event_topic, event_group=event_group)
            asyncio.ensure_future(command_consumer.start())

        return command_consumer

    def create_command_emitter(self, settings, serializer):
        command_emitter = None
        if hasattr(settings, 'EVENTY_COMMAND_EMITTER'):
            self.logger.debug(
                f"Initializing command emitter {settings.EVENTY_COMMAND_EMITTER}")
            if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
                raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

            command_emitter_class = load_class(
                settings.EVENTY_COMMAND_EMITTER)
            command_emitter = command_emitter_class(
                serializer=serializer, bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER, username=settings.KAFKA_USERNAME, password=settings.KAFKA_PASSWORD,)
        return command_emitter

    def create_event_emitter(self, settings, serializer):
        event_emitter = None
        if hasattr(settings, 'EVENTY_EVENT_EMITTER'):
            self.logger.debug(
                f"Initializing event emitter {settings.EVENTY_EVENT_EMITTER}")
            if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
                raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

            default_topic = None
            if hasattr(settings, 'EVENTY_EVENT_EMITTER_DESTINATION'):
                default_topic = settings.EVENTY_EVENT_EMITTER_DESTINATION

            event_emitter_class = load_class(
                settings.EVENTY_EVENT_EMITTER)
            event_emitter = event_emitter_class(
                serializer=serializer, bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER, username=settings.KAFKA_USERNAME, password=settings.KAFKA_PASSWORD, default_topic=default_topic)

        return event_emitter

    def create_event_consumer(self, settings, serializer):
        event_consumer = None
        if hasattr(settings, 'EVENTY_EVENT_CONSUMER'):
            self.logger.debug(
                f"Initializing event consumer {settings.EVENTY_EVENT_CONSUMER}")

            if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
                raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
            if not hasattr(settings, 'EVENTY_EVENT_CONSUMER_SOURCES'):
                raise Exception("Missing EVENTY_EVENT_CONSUMER_SOURCES config")
            event_topics = settings.EVENTY_EVENT_CONSUMER_SOURCES.split(',')

            event_consumer_class = load_class(
                settings.EVENTY_EVENT_CONSUMER)
            event_consumer = event_consumer_class(context=self.context, serializer=serializer, bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVER,
                                                  username=settings.KAFKA_USERNAME, password=settings.KAFKA_PASSWORD, event_topics=event_topics)
            asyncio.ensure_future(event_consumer.start())
        return event_consumer

    def create_http_handler(self, settings):
        if hasattr(settings, 'EVENTY_HTTP_HANDLER_PORT'):
            self.http_handler_port = settings.EVENTY_HTTP_HANDLER_PORT
        else:
            self.http_handler_port = 8000
        self.logger.debug(
            f"Initializing http handler on port {self.http_handler_port}")
        http_handler = Sanic()
        return http_handler

    def register_event_class(self, event_class, event_name):
        if self.context.get('serializer') is None:
            raise Exception(
                'Cannot register event class: no serializer configured')
        self.context.get('serializer').register_event_class(
            event_class=event_class, event_name=event_name)

    def register_command_class(self, command_class, command_name):
        if self.context.get('serializer') is None:
            raise Exception(
                'Cannot register command class: no serializer configured')
        self.context.get('serializer').register_event_class(
            event_class=command_class, event_name=command_name)

    def register_event_handler(self, event_handler: Callable[[BaseEvent, str], None], event_class):
        if self.context.get('event_consumer') is None:
            raise Exception(
                'Cannot register event handler: no event consumer configured')
        self.context.get('event_consumer').register_event_handler(
            event_handler=event_handler, event_class=event_class)

    def register_http_blueprint(self, blueprint):
        self.context.get('http_handler').blueprint(blueprint)
