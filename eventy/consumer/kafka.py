from aiokafka import AIOKafkaConsumer
import logging
import sys
from typing import Any, Dict, Type, Callable, List
import asyncio
from ..serializer.base import BaseEventSerializer
from ..event.base import BaseEvent
from ..command.base import BaseCommand
from .base import BaseEventConsumer, BaseCommandConsumer
from ..context import Context

__all__ = [
    'KafkaEventConsumer',
    'KafkaCommandConsumer'
]


class KafkaEventConsumer(BaseEventConsumer):

    def __init__(self, context: Context, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None, event_topics: List[str] = None, event_group: str = None, position: str = 'earliest'):
        self.logger = logging.getLogger(__name__)

        self.context = context
        self.event_topics = event_topics
        self.event_group = event_group
        self.position = position
        self.consumer = KafkaConsumer(serializer=serializer, bootstrap_servers=bootstrap_servers,
                                      username=username, password=password)

    async def handle_event(self, event: BaseEvent, corr_id: str) -> None:
        await event.handle(context=self.context, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topics=self.event_topics, event_group=self.event_group, event_handler=self.handle_event, position=self.position)


class KafkaCommandConsumer(BaseCommandConsumer):

    def __init__(self, context: Context, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None, event_topic: str = None, event_group: str = None, position: str = 'latest'):
        self.logger = logging.getLogger(__name__)

        self.context = context
        self.position = position
        self.event_topic = event_topic
        self.event_group = event_group
        self.consumer = KafkaConsumer(
            serializer=serializer, bootstrap_servers=bootstrap_servers, username=username, password=password)

    async def handle_command(self, event: BaseCommand, corr_id: str):
        await event.execute(self.context, corr_id=corr_id)

    async def start(self):
        await self.consumer.start(
            event_topics=[self.event_topic], event_group=self.event_group, event_handler=self.handle_command, position=self.position)


class KafkaConsumer:

    def __init__(self, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None):
        self.logger = logging.getLogger(__name__)
        self.consumer = None
        self.serializer = serializer
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.password = password

    async def start(self, event_topics: List[str], event_group: str, event_handler: Callable[[BaseEvent, str], None], position: str):

        consumer_args: Dict[str, Any]
        consumer_args = {
            'loop': asyncio.get_event_loop(),
            'bootstrap_servers': [self.bootstrap_servers],
            'enable_auto_commit': False,
            'group_id': event_group,
            'value_deserializer': self.serializer.decode,
            'auto_offset_reset': position
        }

        if self.username is not None and self.username != '' and self.password is not None and self.password != '':
            consumer_args.update({
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': self.username,
                'sasl_plain_password': self.password,
            })

        try:
            self.logger.info(
                f'Initialize kafka consumer on topic {event_topics} with group {event_group}')
            self.consumer = AIOKafkaConsumer(
                *event_topics, **consumer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {self.bootstrap_servers} : {e}")
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
