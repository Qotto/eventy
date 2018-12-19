from aiokafka import AIOKafkaProducer
import logging
from ..event.base import BaseEvent
from ..command.base import BaseCommand
from ..serializer.base import BaseEventSerializer
from typing import Any, Dict
import asyncio
from .base import BaseEventEmitter, BaseCommandEmitter

__all__ = [
    'KafkaEventEmitter',
    'KafkaCommandEmitter'
]


class KafkaEventEmitter(BaseEventEmitter):

    def __init__(self, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None, default_topic: str = None):
        self.producer = KafkaProducer(
            serializer=serializer, bootstrap_servers=bootstrap_servers, username=username, password=password)
        self.default_topic = default_topic

    async def send(self, event: BaseEvent, destination: str = None):
        if destination:
            event_topic = destination
        else:
            event_topic = self.default_topic
        await self.producer.send(event=event, event_topic=event_topic)


class KafkaCommandEmitter(BaseCommandEmitter):
    def __init__(self, settings: object, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None):
        self.producer = KafkaProducer(serializer=serializer,
                                      bootstrap_servers=bootstrap_servers, username=username, password=password)

    async def send(self, command: BaseCommand, destination: str):
        await self.producer.send(event=command, event_topic=destination)


class KafkaProducer:

    def __init__(self, serializer: BaseEventSerializer, bootstrap_servers: str, username: str = None, password: str = None):
        self.logger = logging.getLogger(__name__)

        self.serializer = serializer
        self.started = False
        try:
            producer_args: Dict[str, Any]
            producer_args = {
                'loop': asyncio.get_event_loop(),
                'bootstrap_servers': [bootstrap_servers],
                'value_serializer': self.serializer.encode
            }
            if username is not None and username != '' and password is not None and password != '':
                producer_args.update({
                    'sasl_mechanism': 'PLAIN',
                    'sasl_plain_username': username,
                    'sasl_plain_password': password,
                })

            self.producer = AIOKafkaProducer(**producer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {bootstrap_servers}")
            raise e

    async def send(self, event: BaseEvent, event_topic: str):
        if not self.started:
            await self.producer.start()
            self.started = True
        await self.producer.send_and_wait(event_topic, event)
