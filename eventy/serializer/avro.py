# coding: utf-8
# Copyright (c) Qotto, 2018

import json
import os
import yaml

import logging
from ..event.base import BaseEvent
from ..event.generic import GenericEvent
from .base import BaseEventSerializer
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from avro.schema import NamedSchema, Parse
from io import BytesIO
import re

from typing import Dict, Type

__all__ = [
    'AvroEventSerializer',
]


class AvroEventSerializer(BaseEventSerializer):
    AVRO_SCHEMA_FILE_EXTENSION = 'avsc.yaml'

    def __init__(self, settings: object) -> None:
        self.logger = logging.getLogger(__name__)
        self._schemas: Dict[str, NamedSchema] = dict()
        self._events: Dict[object, Type[BaseEvent]] = dict()

        if not hasattr(settings, 'AVRO_SCHEMAS_FOLDER'):
            raise Exception('Missing AVRO_SCHEMAS_FOLDER config')
        self.scan_folder(schemas_folder=settings.AVRO_SCHEMAS_FOLDER)

    def scan_folder(self, schemas_folder: str) -> None:
        """
        Searches Avro schema files in the specified folder.
        The schema files should have the avsc.yaml extension.
        """
        with os.scandir(schemas_folder) as entries:  # type: ignore
            for entry in entries:
                # ignores everything that is not a file
                if not entry.is_file():
                    continue
                # ignores hidden files
                if entry.name.startswith('.'):
                    continue
                # ignores everything files with wrong extension
                if not entry.name.endswith(f'.{self.AVRO_SCHEMA_FILE_EXTENSION}'):
                    continue
                self._extract_schema_from_file(entry.path)

    def _extract_schema_from_file(self, file_path: str) -> None:
        with open(file_path, 'r') as f:
            for s in yaml.load_all(f):
                avro_schema_data = json.dumps(s)
                avro_schema = Parse(avro_schema_data)
                schema_name = avro_schema.namespace + '.' + avro_schema.name
                if schema_name in self._schemas:
                    raise Exception(
                        f"Avro schema {schema_name} was defined more than once!")
                self._schemas[schema_name] = avro_schema

    def register_event_class(self, event_class: Type[BaseEvent], event_name: str) -> None:
        """
        Registers an event class associated to a particular event name.
        """

        # name can be a regex
        event_name_regex = re.compile(event_name)

        match = False
        for schema_name in self._schemas:
            if event_name_regex.match(schema_name):
                match = True
                break
        if not match:
            raise NameError(f"{event_name} does not match any schema")

        self._events[event_name_regex] = event_class

    def encode(self, event: BaseEvent) -> bytes:
        schema = self._schemas[event.name]

        if schema is None:
            raise NameError(
                f"No schema found to encode event with name {event.name}")

        output = BytesIO()
        writer = DataFileWriter(output, DatumWriter(), schema)
        writer.append(event.data)
        writer.flush()
        encoded_event = output.getvalue()
        writer.close()
        return encoded_event

    def decode(self, encoded_event: bytes) -> BaseEvent:
        reader = DataFileReader(BytesIO(encoded_event), DatumReader())
        schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
        schema_name = schema['namespace'] + '.' + schema['name']
        event_data = next(reader)

        # finds a matching event name
        event_class = None
        for e_name, e_class in self._events.items():
            if e_name.match(schema_name):
                event_class = e_class
                break

        if event_class is None:
            event_class = GenericEvent

        return event_class.from_data(event_name=schema_name, event_data=event_data)
