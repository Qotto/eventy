from aiokafka import AIOKafkaProducer
import logging
from ..event.base import BaseEvent
from ..command.base import BaseCommand
from ..serializer.base import BaseEventSerializer
from typing import Any, Dict
import asyncio
from .base import BaseEventEmitter

__all__ = [
    'KafkaProducer'
]


class KafkaProducer(BaseEventEmitter):

    def __init__(self, settings: object, serializer: BaseEventSerializer)->None:
        self.logger = logging.getLogger(__name__)

        if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

        self.serializer = serializer
        self.started = False
        try:
            producer_args: Dict[str, Any]
            producer_args = {
                'loop': asyncio.get_event_loop(),
                'bootstrap_servers': [settings.KAFKA_BOOTSTRAP_SERVER],
                'value_serializer': self.serializer.encode
            }

            self.producer = AIOKafkaProducer(**producer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {settings.KAFKA_BOOTSTRAP_SERVER}")
            raise e

    async def send(self, event: BaseEvent, destination: str, key=None):
        if not self.started:
            await self.producer.start()
            self.started = True
        self.logger.debug(
            f'Sending event {event.name} on topic {destination}')
        await self.producer.send_and_wait(destination, event, key)
