import logging

import asyncio
from sanic import Sanic, Blueprint
from sanic.response import json
from typing import Type, Callable, Dict, List

from ..utils import load_class
from ..command.base import BaseCommand
from ..event.base import BaseEvent
from ..serializer.base import BaseEventSerializer
from ..emitter.base import BaseEventEmitter
from ..consumer.base import BaseEventConsumer
from .base import BaseApp

health_bp = Blueprint('health')


@health_bp.route('/health')
async def health(request):
    return json({}, status=200)


class Eventy(BaseApp):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.context: Dict[str, object] = dict()
        self.http_handler = None
        self.http_handler_port = None

    def start(self):
        if self.http_handler is not None:
            asyncio.ensure_future(self.http_handler.create_server(
                host="0.0.0.0", port=self.http_handler_port, debug=self.http_debug, access_log=self.http_access_log))

        asyncio.get_event_loop().run_forever()

    def configure_event_serializer(self, settings: object, serializer_name: str) -> BaseEventSerializer:
        if not hasattr(settings, 'EVENTY_EVENT_SERIALIZER'):
            raise Exception('Missing EVENTY_EVENT_SERIALIZER config')
        self.logger.debug(
            f"Configure serializer {settings.EVENTY_EVENT_SERIALIZER}")
        serializer_class = load_class(settings.EVENTY_EVENT_SERIALIZER)
        serializer = serializer_class(settings=settings)
        self.set(serializer_name, serializer)
        return serializer

    def configure_consumer(self, settings: object, serializer: BaseEventSerializer, consumer_name: str, event_topics: List[str], event_group: str, position: str)-> BaseEventConsumer:
        if not hasattr(settings, 'EVENTY_EVENT_CONSUMER'):
            raise Exception('Missing EVENTY_EVENT_CONSUMER config')
        self.logger.debug(
            f"Initializing consumer {settings.EVENTY_EVENT_CONSUMER}")
        consumer_class = load_class(
            settings.EVENTY_EVENT_CONSUMER)
        consumer = consumer_class(settings=settings, app=self,
                                  serializer=serializer,  event_topics=event_topics, event_group=event_group, position=position)
        asyncio.ensure_future(consumer.start())
        self.set(consumer_name, consumer)
        return consumer

    def configure_emitter(self, settings: object, serializer: BaseEventSerializer, emitter_name: str)->BaseEventEmitter:
        if not hasattr(settings, 'EVENTY_EVENT_EMITTER'):
            raise Exception('Missing EVENTY_EVENT_EMITTER config')
        self.logger.debug(
            f"Initializing emitter {settings.EVENTY_EVENT_EMITTER}")
        emitter_class = load_class(
            settings.EVENTY_EVENT_EMITTER)
        emitter = emitter_class(settings=settings,
                                serializer=serializer)
        self.set(emitter_name, emitter)
        return emitter

    def configure_http_handler(self, settings: object, http_handler_name: str) -> Sanic:
        if hasattr(settings, 'EVENTY_HTTP_HANDLER_PORT'):
            self.http_handler_port = settings.EVENTY_HTTP_HANDLER_PORT
        else:
            self.http_handler_port = 8000

        if hasattr(settings, 'EVENTY_HTTP_ACCESS_LOG'):
            self.http_access_log = settings.EVENTY_HTTP_ACCESS_LOG
        else:
            self.http_access_log = False

        if hasattr(settings, 'EVENTY_HTTP_DEBUG'):
            self.http_debug = settings.EVENTY_HTTP_DEBUG
        else:
            self.http_debug = False

        self.logger.debug(
            f"Initializing http handler on port {self.http_handler_port}")
        self.http_handler = Sanic()

        self.http_handler.request_middleware.append(
            self.attach_app_to_http_request)

        self.http_handler.blueprint(health_bp)

        self.set(http_handler_name, self.http_handler)

        return self.http_handler

    def attach_app_to_http_request(self, request):
        request['app'] = self

    def set(self, object_name: str, object_instance: object):
        self.context[object_name] = object_instance

    def get(self, object_name: str):
        return self.context.get(object_name)
