# coding: utf-8
# Copyright (c) Qotto, 2018

import json
import os
import yaml

from ..event.base import BaseEvent
from ..event.generic import GenericEvent
from .base import BaseEventSerializer
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from avro.schema import NamedSchema, Parse
from io import BytesIO

from typing import Dict, Type

__all__ = [
    'AvroEventSerializer',
]


class AvroEventSerializer(BaseEventSerializer):
    AVRO_SCHEMA_FILE_EXTENSION = 'avsc.yaml'

    def __init__(self) -> None:
        self._schemas: Dict[str, NamedSchema] = dict()
        self._events: Dict[str, Type[BaseEvent]] = dict()

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
            avro_schema_data = json.dumps(yaml.load(f))
            avro_schema = Parse(avro_schema_data)
            if avro_schema.name in self._schemas:
                raise Exception(f"Avro schema {avro_schema.name} was defined more than once!")
            self._schemas[avro_schema.name] = avro_schema

    def register_event_class(self, event_class: Type[BaseEvent], event_name: str = None) -> None:
        """
        Registers an event class associated to a particular event name.
        """
        if event_name is None:
            event_name = event_class.__name__
        if event_name not in self._schemas:
            raise NameError(f"{event_name} event does not exist")
        self._events[event_name] = event_class
        raise NotImplementedError("This functionality is planned for future.")

    def encode(self, event: BaseEvent) -> bytes:
        schema = self._schemas[event.name]
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
        event_name = schema['name']
        event_data = next(reader)
        event_class = self._events.get(event_name, GenericEvent)
        return event_class.from_data(event_name, event_data)
