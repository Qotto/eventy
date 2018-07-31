# coding: utf-8
# Copyright (c) Qotto, 2018

import logging

from ..event.base import BaseEvent
from ..serializer.base import BaseEventSerializer
from .base import BaseEventStore
from kafka import KafkaProducer
from kafka import KafkaConsumer

from typing import Any, Dict, List

__all__ = [
    'KafkaEventStore',
]

logger = logging.getLogger(__name__)


class KafkaEventStore(BaseEventStore):
    """
    An event store that uses Kafka for persistance.
    """
    def __init__(
        self, serializer: BaseEventSerializer,
        bootstrap_servers: List[str], username: str = None, password: str = None,
    ) -> None:
        super().__init__(serializer)
        self._producer: KafkaProducer = None
        self._consumer: KafkaConsumer = None
        self._default_write_topic: str = None
        self._kafka_args: Dict[str, Any] = dict()
        self._kafka_args.update({
            'bootstrap_servers': bootstrap_servers,
        })
        if username and password:
            self._kafka_args.update({
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': username,
                'sasl_plain_password': password,
            })

    def register_producer(self, default_write_topic: str = None):
        """
        Registers a Kafka producer. Required to be able to call the `write` method.
        """
        try:
            self._default_write_topic = default_write_topic
            self._producer = KafkaProducer(acks=1, **self._kafka_args)
        except Exception:
            logger.exception(f"Cannot establish connection with Kafka brokers {self._kafka_args['bootstrap_servers']}")
            raise

    def register_consumer(self, group_id: str, topics: str, **args):
        """
        Registers a Kafka consumer. Required to be able to call the `read` method.
        """
        topics_list = [topic.strip() for topic in topics.split(',')]
        try:
            self._consumer = KafkaConsumer(*topics_list, group_id=group_id, **self._kafka_args, **args)
        except Exception:
            logger.exception(f"Cannot establish connection with Kafka brokers {self._kafka_args['bootstrap_servers']}")
            raise

    def read(self) -> BaseEvent:
        if self._consumer is None:
            raise RuntimeError("Kafka consumer is not configured. Please call register_consumer first.")
        serialized_event = next(self._consumer).value
        decoded_event = self.serializer.decode(serialized_event)
        return decoded_event

    def write(self, event: BaseEvent, key: str = None, topic: str = None):
        if self._producer is None:
            raise RuntimeError("Kakfa producer is not configured. Please call register_producer first.")
        if topic is None:
            topic = self._default_write_topic
        serialized_event = self.serializer.encode(event)
        try:
            self._producer.send(topic, serialized_event, key=key)
            self._producer.flush()
        except Exception:
            logger.exception("Failed to dispatch data to Kafka")
            raise
