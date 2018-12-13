from aiokafka import AIOKafkaConsumer
import logging
import sys
from typing import Any, Dict, Type
import asyncio
from ..serializer.avro import AvroEventSerializer
from ..event.handler import EventHandler
from ..event.base import BaseEvent
from ..service.command import BaseCommand
from ..service.service import Service
from .base import BaseEventConsumer, BaseCommandConsumer
from ..serializer.base import BaseEventSerializer


__all__ = [
    'KafkaEventConsumer',
    'KafkaCommandConsumer'
]


class KafkaEventConsumer(BaseEventConsumer, EventHandler):

    def __init__(self, settings: Any, serializer: BaseEventSerializer, event_topic: str, event_group: str = None, position: str = 'earliest'):
        self.logger = logging.getLogger(__name__)
        self.consumer = KafkaConsumer(settings=settings, serializer=serializer)
        self.event_topic = event_topic
        self.event_group = event_group
        self.position = position
        self.handlers = dict()

    async def handle_event(self, event: BaseEvent, corr_id: str):

        handler = self.handlers.get(type(event))
        if handler:
            await handler.handle_event(event=event, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topic=self.event_topic, event_group=self.event_group, event_handler=self, position=self.position)

    def register_event_handler(self, event_handler: EventHandler, event_class: Type[BaseEvent]):
        self.handlers[event_class] = event_handler


class KafkaCommandConsumer(BaseCommandConsumer, EventHandler):

    def __init__(self, settings: Any, serializer: BaseEventSerializer, event_topic: str, event_group: str, position: str = 'latest'):
        self.logger = logging.getLogger(__name__)
        self.consumer = KafkaConsumer(settings=settings, serializer=serializer)
        self.event_topic = event_topic
        self.event_group = event_group
        self.position = position
        self.services = dict()

    async def handle_event(self, event: BaseEvent, corr_id: str):
        service = self.services.get(type(event))
        if service:
            await service.execute(command=event, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topic=self.event_topic, event_group=self.event_group, event_handler=self, position=self.position)

    def register_service(self, service: Service, command_class: Type[BaseCommand]):
        self.services[command_class] = service


class KafkaConsumer:

    def __init__(self, settings: Any, serializer: BaseEventSerializer):
        self.logger = logging.getLogger(__name__)
        self.consumer = None
        self.serializer = serializer
        self.settings = settings

        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

    async def start(self, event_topic: str, event_group: str, event_handler: EventHandler, position: str):

        consumer_args: Dict[str, Any]
        consumer_args = {
            'loop': asyncio.get_event_loop(),
            'bootstrap_servers': [self.settings.KAFKA_BOOTSTRAP_SERVER],
            'enable_auto_commit': False,
            'group_id': event_group,
            'value_deserializer': self.serializer.decode,
            'auto_offset_reset': position
        }
        if self.settings.KAFKA_USERNAME != '' and self.settings.KAFKA_PASSWORD != '':
            consumer_args.update({
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': self.settings.KAFKA_USERNAME,
                'sasl_plain_password': self.settings.KAFKA_PASSWORD,
            })

        try:
            self.logger.info(
                f'Initialize kafka consumer on topic {event_topic}')
            self.consumer = AIOKafkaConsumer(
                event_topic, **consumer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {self.settings.KAFKA_BOOTSTRAP_SERVER} : {e}")
            raise e

        self.logger.info(
            f'Starting kafka consumer on topic {event_topic}')
        try:
            await self.consumer.start()
        except Exception as e:
            self.logger.error(
                f'An error occurred while starting kafka consumer : {e}')
            sys.exit(1)

        try:
            async for msg in self.consumer:
                event = msg.value

                corr_id = event.data['correlation_id']

                await event_handler.handle_event(event=event, corr_id=corr_id)

                if event_group is not None:
                    await self.consumer.commit()
        except Exception as e:
            self.logger.error(
                f'An error occurred while handling received message : {e}')
            raise e
        finally:
            await self.consumer.stop()
