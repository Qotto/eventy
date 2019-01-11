from aiokafka import AIOKafkaConsumer
import logging
import sys
from typing import Any, Dict, Type, Callable, List
import asyncio
from ..serializer.base import BaseEventSerializer
from ..event.base import BaseEvent
from ..command.base import BaseCommand
from .base import BaseEventConsumer
from ..app.base import BaseApp

__all__ = [
    'KafkaConsumer'
]


class KafkaConsumer(BaseEventConsumer):

    def __init__(self, settings: object, app: BaseApp, serializer: BaseEventSerializer, event_topics: List[str], event_group: str, position: str) -> None:
        self.logger = logging.getLogger(__name__)

        if not hasattr(settings, 'KAFKA_BOOTSTRAP_SERVER'):
            raise Exception('Missing KAFKA_BOOTSTRAP_SERVER config')

        self.max_retries = 10
        if hasattr(settings, 'EVENTY_CONSUMER_MAX_RETRIES'):
            self.max_retries = settings.EVENTY_CONSUMER_MAX_RETRIES

        self.retry_interval = 1000
        if hasattr(settings, 'EVENTY_CONSUMER_RETRY_INTERVAL'):
            self.retry_interval = settings.EVENTY_CONSUMER_RETRY_INTERVAL

        self.retry_backoff_coeff = 2
        if hasattr(settings, 'EVENTY_CONSUMER_RETRY_BACKOFF_COEFF'):
            self.retry_backoff_coeff = settings.EVENTY_CONSUMER_RETRY_BACKOFF_COEFF

        self.app = app
        self.event_topics = event_topics
        self.event_group = event_group
        self.position = position
        self.consumer = None
        self.serializer = serializer
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVER

    async def start(self):

        consumer_args: Dict[str, Any]
        consumer_args = {
            'loop': asyncio.get_event_loop(),
            'bootstrap_servers': [self.bootstrap_servers],
            'enable_auto_commit': False,
            'group_id': self.event_group,
            'value_deserializer': self.serializer.decode,
            'auto_offset_reset': self.position
        }

        try:
            self.consumer = AIOKafkaConsumer(
                *self.event_topics, **consumer_args)
        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {self.bootstrap_servers} : {e}")
            raise e

        self.logger.info(
            f'Starting kafka consumer on topic {self.event_topics} with group {self.event_group}')
        try:
            await self.consumer.start()
        except Exception as e:
            self.logger.error(
                f'An error occurred while starting kafka consumer on topic {self.event_topics} with group {self.event_group}: {e}')
            sys.exit(1)

        async for msg in self.consumer:

            retries = 0
            sleep_duration_in_ms = self.retry_interval
            while True:
                try:
                    event = msg.value
                    corr_id = event.correlation_id

                    self.logger.info(
                        f"[CID:{corr_id}] Start handling {event.name}")
                    await event.handle(app=self.app, corr_id=corr_id)
                    self.logger.info(
                        f"[CID:{corr_id}] End handling {event.name}")

                    if self.event_group is not None:
                        self.logger.debug(
                            f"[CID:{corr_id}] Commit Kafka transaction")
                        await self.consumer.commit()

                    self.logger.debug(
                        f"[CID:{corr_id}] Continue with the next message")
                    # break the retry loop
                    break

                except Exception as e:
                    self.logger.error(
                        f'[CID:{corr_id}] An error occurred while handling received message : {e}.')

                    if retries != self.max_retries:
                        # increase the number of retries
                        retries = retries + 1

                        sleep_duration_in_s = int(sleep_duration_in_ms/1000)
                        self.logger.info(
                            f"[CID:{corr_id}] Sleeping {sleep_duration_in_s}s a before retrying...")
                        await asyncio.sleep(sleep_duration_in_s)

                        # increase the sleep duration
                        sleep_duration_in_ms = sleep_duration_in_ms * self.retry_backoff_coeff

                    else:
                        self.logger.error(
                            f'[CID:{corr_id}] Unable to handle message within {1 + self.max_retries} tries. Stopping process')
                        sys.exit(1)
