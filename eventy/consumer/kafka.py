from aiokafka import AIOKafkaConsumer
import logging
import sys
from typing import Any, Dict, Type, Callable, List
import asyncio
from ..serializer.avro import AvroEventSerializer
from ..event.base import BaseEvent
from ..service.command import BaseCommand
from .base import BaseEventConsumer, BaseCommandConsumer
from ..runtime import runtime_context


__all__ = [
    'KafkaEventConsumer',
    'KafkaCommandConsumer'
]


class KafkaEventConsumer(BaseEventConsumer):

    def __init__(self, settings: object, event_topics: List[str] = None, event_group: str = None, position: str = 'earliest'):
        self.logger = logging.getLogger(__name__)
        self.consumer = KafkaConsumer(settings=settings)

        if event_topics:
            self.event_topics = event_topics
        else:
            self.event_topics = settings.EVENTY_EVENT_CONSUMER_SOURCES.split(
                ',')

        self.event_group = event_group
        self.position = position
        self.handlers = dict()

    async def handle_event(self, event: BaseEvent, corr_id: str) -> None:

        handler = self.handlers.get(type(event))
        if handler:
            await handler(event=event, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topics=self.event_topics, event_group=self.event_group, event_handler=self.handle_event, position=self.position)

    def register_event_handler(self, event_handler: Callable[[BaseEvent, str], None], event_class: Type[BaseEvent]):
        self.logger.debug(
            f"Registering event handler {event_handler} for event {event_class}")
        self.handlers[event_class] = event_handler


class KafkaCommandConsumer(BaseCommandConsumer):

    def __init__(self, settings: object, event_topic: str = None, event_group: str = None, position: str = 'latest'):
        self.logger = logging.getLogger(__name__)
        self.consumer = KafkaConsumer(settings=settings)

        if event_topic:
            self.event_topic = event_topic
        else:
            self.event_topic = settings.KAFKA_COMMAND_TOPIC

        if event_group:
            self.event_group = event_group
        else:
            self.event_group = settings.KAFKA_COMMAND_GROUP

        self.position = position
        self.command_handlers = dict()

    async def handle_event(self, event: BaseEvent, corr_id: str):
        command_handler = self.command_handlers.get(type(event))
        if command_handler:
            await command_handler(command=event, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topics=[self.event_topic], event_group=self.event_group, event_handler=self.handle_event, position=self.position)

    def register_command_handler(self, command_handler: Callable[[BaseCommand, str], None], command_class: Type[BaseCommand]):
        self.logger.debug(
            f"Registering command handler {command_handler} for command {command_class}")
        self.command_handlers[command_class] = command_handler


class KafkaConsumer:

    def __init__(self, settings: Any):
        self.logger = logging.getLogger(__name__)
        self.consumer = None
        self.serializer = runtime_context.serializer
        self.settings = settings

        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

    async def start(self, event_topics: List[str], event_group: str, event_handler: Callable[[BaseEvent, str], None], position: str):

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
                f'Initialize kafka consumer on topic {event_topics}')
            self.consumer = AIOKafkaConsumer(
                *event_topics, **consumer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {self.settings.KAFKA_BOOTSTRAP_SERVER} : {e}")
            raise e

        self.logger.info(
            f'Starting kafka consumer on topic {event_topics}')
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

                await event_handler(event=event, corr_id=corr_id)

                if event_group is not None:
                    await self.consumer.commit()
        except Exception as e:
            self.logger.error(
                f'An error occurred while handling received message : {e}')
            raise e
        finally:
            await self.consumer.stop()
