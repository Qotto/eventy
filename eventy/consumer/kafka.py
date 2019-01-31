from aiokafka import AIOKafkaConsumer, TopicPartition
import logging
import sys
import traceback

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
        self.current_position_checkpoint_callback = None
        self.end_position_checkpoint_callback = None
        bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVER

        consumer_args: Dict[str, Any]
        consumer_args = {
            'loop': asyncio.get_event_loop(),
            'bootstrap_servers': [bootstrap_servers],
            'enable_auto_commit': False,
            'group_id': self.event_group,
            'value_deserializer': serializer.decode,
            'auto_offset_reset': self.position
        }

        try:
            self.consumer = AIOKafkaConsumer(
                *self.event_topics, **consumer_args)

        except Exception as e:
            self.logger.error(
                f"Unable to connect to the Kafka broker {bootstrap_servers} : {e}")
            raise e

    def set_current_position_checkpoint_callback(self, checkpoint_callback):
        self.current_position_checkpoint_callback = checkpoint_callback

    def set_end_position_checkpoint_callback(self, checkpoint_callback):
        self.end_position_checkpoint_callback = checkpoint_callback

    async def current_position_checkpoint(self):
        checkpoint = {}
        for partition in self.consumer.assignment():
            offset = await self.consumer.committed(partition) or 0
            checkpoint[partition] = offset

        self.logger.info(
            f'Current position checkpoint created for consumer : {checkpoint}')

        return checkpoint

    async def end_position_checkpoint(self):
        checkpoint = {}
        for partition in self.consumer.assignment():
            offset = (await self.consumer.end_offsets([partition]))[partition]
            checkpoint[partition] = offset

        self.logger.info(
            f'End position checkpoint created for consumer : {checkpoint}')

        return checkpoint

    async def is_checkpoint_reached(self, checkpoint):
        for partition in self.consumer.assignment():
            position = (await self.consumer.position(partition))
            if position < checkpoint[partition]:
                return False
        return True

    async def start(self):

        self.logger.info(
            f'Starting kafka consumer on topic {self.event_topics} with group {self.event_group}')
        try:
            await self.consumer.start()
        except Exception as e:
            self.logger.error(
                f'An error occurred while starting kafka consumer on topic {self.event_topics} with group {self.event_group}: {e}')
            sys.exit(1)

        current_position_checkpoint = None
        end_position_checkpoint = None
        if self.position == 'earliest' and self.event_group is not None:
            current_position_checkpoint = await self.current_position_checkpoint()
            end_position_checkpoint = await self.end_position_checkpoint()

            await self.consumer.seek_to_beginning()

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
                        f'[CID:{corr_id}] An error occurred while handling received message : {traceback.format_exc()}.')

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

            if current_position_checkpoint and await self.is_checkpoint_reached(current_position_checkpoint):
                self.logger.info('Current position checkpoint reached')
                if self.current_position_checkpoint_callback:
                    await self.current_position_checkpoint_callback()
                current_position_checkpoint = None

            if end_position_checkpoint and await self.is_checkpoint_reached(end_position_checkpoint):
                self.logger.info('End position checkpoint reached')
                if self.end_position_checkpoint_callback:
                    await self.end_position_checkpoint_callback()
                end_position_checkpoint = None
