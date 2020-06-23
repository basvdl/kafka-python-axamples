import io
from pathlib import Path
from typing import Optional, Dict, ByteString

import avro
from avro.io import DatumWriter, DatumReader
from confluent_kafka.serialization import SerializationContext


class AvroSerDe:
    def __init__(self, message_schema_path: Path, key_schema_path: Optional[Path]):
        self.message_schema_path = message_schema_path
        self.key_schema_path = key_schema_path


class AvroSerializer(AvroSerDe):
    def __call__(self, content: Dict, obj: SerializationContext) -> ByteString:
        if obj.field == 'key':
            return self._serialize_message(content, self.key_schema_path)
        else:
            return self._serialize_message(content, self.message_schema_path)

    @staticmethod
    def _serialize_message(content, schema_path: Path) -> ByteString:
        schema = avro.schema.parse(schema_path.read_text())

        bytes_writer = io.BytesIO()
        writer = DatumWriter(schema)
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(content, encoder)

        return bytes_writer.getvalue()


class AvroDeserializer(AvroSerDe):
    def __call__(self, content: Dict, obj: SerializationContext) -> ByteString:
        if obj.field == 'key':
            return self._deserialize_message(content, self.key_schema_path)
        else:
            return self._deserialize_message(content, self.message_schema_path)

    @staticmethod
    def _deserialize_message(content, schema_path: Path) -> ByteString:
        schema = avro.schema.parse(schema_path.read_text())

        bytes_reader = io.BytesIO(content)
        reader = DatumReader(schema)
        decoder = avro.io.BinaryDecoder(bytes_reader)

        return reader.read(decoder)
