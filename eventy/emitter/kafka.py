from aiokafka import AIOKafkaProducer
import logging
from ..event.base import BaseEvent
from ..service.command import BaseCommand
from ..serializer.base import BaseEventSerializer
from typing import Any, Dict
import asyncio
from .base import BaseEventEmitter, BaseCommandEmitter


__all__ = [
    'KafkaEventEmitter',
    'KafkaCommandEmitter'
]


class KafkaEventEmitter(BaseEventEmitter):
    def __init__(self, settings: Any, serializer: BaseEventSerializer):
        self.producer = KafkaProducer(settings=settings, serializer=serializer)

    async def send(self, event: BaseEvent, destination: str):
        await self.producer.send(event=event, event_topic=destination)


class KafkaCommandEmitter(BaseCommandEmitter):
    def __init__(self, settings: Any, serializer: BaseEventSerializer):
        self.producer = KafkaProducer(settings=settings, serializer=serializer)

    async def send(self, command: BaseCommand, destination: str):
        await self.producer.send(event=command, event_topic=destination)


class KafkaProducer:

    def __init__(self, settings: Any, serializer: BaseEventSerializer):
        self.logger = logging.getLogger(__name__)
        self.producer = self.create_producer()
        self.serializer = serializer
        self.started = False

        self.settings = settings
        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')
        if self.settings.KAFKA_BOOTSTRAP_SERVER is None:
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

    async def send(self, event: BaseEvent, event_topic: str):
        if not self.started:
            await self.producer.start()
            self.started = True
        await self.producer.send_and_wait(event_topic, event)

    def create_producer(self) -> AIOKafkaProducer:
        try:

            producer_args: Dict[str, Any]
            producer_args = {
                'loop': asyncio.get_event_loop(),
                'bootstrap_servers': [self.settings.KAFKA_BOOTSTRAP_SERVER],
                'value_serializer': self.serializer.encode
            }
            if self.settings.KAFKA_USERNAME != '' and self.settings.KAFKA_PASSWORD != '':
                producer_args.update({
                    'sasl_mechanism': 'PLAIN',
                    'sasl_plain_username': self.settings.KAFKA_USERNAME,
                    'sasl_plain_password': self.settings.KAFKA_PASSWORD,
                })

            return AIOKafkaProducer(**producer_args)
        except Exception as e:
            self.logger.error(
                f"Unabale to connect to the Kafka broker {self.settings.KAFKA_BOOTSTRAP_SERVER}")
            raise e
