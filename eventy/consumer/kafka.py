# coding: utf-8
# Copyright (c) Qotto, 2018-2020

import asyncio
import logging
import sys

from aiokafka import AIOKafkaConsumer
from typing import Any, Dict, List

from ..app.base import BaseApp
from ..serializer.base import BaseEventSerializer
from .base import BaseEventConsumer

__all__ = [
    'KafkaConsumer'
]

logger = logging.getLogger(__name__)


class KafkaConsumer(BaseEventConsumer):
    def __init__(
        self,
        settings: object,
        app: BaseApp,
        serializer: BaseEventSerializer,
        event_topics: List[str],
        event_group: str,
        position: str,
    ) -> None:
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
            logger.error(
                f"Unable to connect to the Kafka broker {bootstrap_servers} : {e}")
            raise e

    def set_current_position_checkpoint_callback(self, checkpoint_callback):
        self.current_position_checkpoint_callback = checkpoint_callback

    def set_end_position_checkpoint_callback(self, checkpoint_callback):
        self.end_position_checkpoint_callback = checkpoint_callback

    async def current_position(self):
        # Warning: this method returns last committed offsets for each assigned partition
        position = {}
        for partition in self.consumer.assignment():
            offset = await self.consumer.committed(partition) or 0
            position[partition] = offset
        return position

    async def consumer_position(self):
        # Warning: this method returns current offsets for each assigned partition
        position = {}
        for partition in self.consumer.assignment():
            position[partition] = await self.consumer.position(partition)
        return position

    async def end_position(self):
        position = {}
        for partition in self.consumer.assignment():
            offset = (await self.consumer.end_offsets([partition]))[partition]
            position[partition] = offset
        return position

    async def is_checkpoint_reached(self, checkpoint):
        for partition in self.consumer.assignment():
            position = (await self.consumer.position(partition))
            if position < checkpoint[partition]:
                return False
        return True

    async def start(self):
        logger.info(
            f'Starting kafka consumer on topic {self.event_topics} with group {self.event_group}'
        )
        try:
            await self.consumer.start()
        except Exception as e:
            logger.error(
                f'An error occurred while starting kafka consumer '
                f'on topic {self.event_topics} with group {self.event_group}: {e}'
            )
            sys.exit(1)

        current_position_checkpoint = None
        end_position_checkpoint = None
        if self.event_group is not None:
            current_position = await self.current_position()
            end_position = await self.end_position()
            logger.debug(f'Current position : {current_position}')
            logger.debug(f'End position : {end_position}')

            if self.position == 'earliest' and self.event_group is not None:
                current_position_checkpoint = current_position
                end_position_checkpoint = end_position
                await self.consumer.seek_to_beginning()

        async for msg in self.consumer:
            retries = 0
            sleep_duration_in_ms = self.retry_interval
            while True:
                try:
                    event = msg.value
                    corr_id = event.correlation_id

                    logger.info(
                        f"[CID:{corr_id}] Start handling {event.name}")
                    await event.handle(app=self.app, corr_id=corr_id)
                    logger.info(
                        f"[CID:{corr_id}] End handling {event.name}")

                    if self.event_group is not None:
                        logger.debug(
                            f"[CID:{corr_id}] Commit Kafka transaction")
                        await self.consumer.commit()

                    logger.debug(
                        f"[CID:{corr_id}] Continue with the next message")
                    # break the retry loop
                    break
                except Exception:
                    logger.exception(
                        f'[CID:{corr_id}] An error occurred while handling received message.'
                    )

                    if retries != self.max_retries:
                        # increase the number of retries
                        retries = retries + 1

                        sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                        logger.info(
                            f"[CID:{corr_id}] Sleeping {sleep_duration_in_s}s a before retrying...")
                        await asyncio.sleep(sleep_duration_in_s)

                        # increase the sleep duration
                        sleep_duration_in_ms = sleep_duration_in_ms * self.retry_backoff_coeff

                    else:
                        logger.error(
                            f'[CID:{corr_id}] Unable to handle message within {1 + self.max_retries} tries. Stopping process')
                        sys.exit(1)

            if current_position_checkpoint and await self.is_checkpoint_reached(current_position_checkpoint):
                logger.info('Current position checkpoint reached')
                if self.current_position_checkpoint_callback:
                    await self.current_position_checkpoint_callback()
                current_position_checkpoint = None

            if end_position_checkpoint and await self.is_checkpoint_reached(end_position_checkpoint):
                logger.info('End position checkpoint reached')
                if self.end_position_checkpoint_callback:
                    await self.end_position_checkpoint_callback()
                end_position_checkpoint = None
