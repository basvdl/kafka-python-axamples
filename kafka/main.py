import collections
import logging
from datetime import datetime

from kafka.kafka_utils import KafkaProducer, KafkaConsumer, KafkaSerializingProducer, KafkaDeserializingConsumer
from kafka import config as config
from kafka.serde import AvroSerializer, AvroDeserializer

logger = logging.getLogger(__file__)
logging.basicConfig(level=config.log_level)


def produce_messages():
    producer_config = {
        'bootstrap.servers': config.kafka_broker,
        # 'queue.buffering.max.messages': 1,
    }
    producer = KafkaProducer(config=producer_config)

    try:
        for i in range(1, 6):
            now = datetime.now()
            message = now.strftime(f"%d/%m/%Y %H:%M:%S - message number {i}")

            producer.produce_message(config.topic, message=message, key=str(i))
    finally:
        producer.shutdown()


def consume_messages():
    consumer_config = {
        'bootstrap.servers': config.kafka_broker,
        'group.id': 'test_consumer',
        'auto.offset.reset': 'earliest'
    }

    consumer = KafkaConsumer(topics=config.topic.split(), config=consumer_config)

    try:
        for message in consumer.consume_message():
            logger.info(f"Key: {message.key()}")
            logger.info(f"Message: {message.value()}")
    finally:
        consumer.shutdown()


def produce_serialized_messages():
    avro_serializer = AvroSerializer(message_schema_path=config.schema_file, key_schema_path=config.schema_file)

    producer_config = {
        'bootstrap.servers': config.kafka_broker,
        'value.serializer': avro_serializer,
        'key.serializer': avro_serializer,
    }
    serialized_producer = KafkaSerializingProducer(config=producer_config)

    try:
        for i in range(1, 3):
            message = {"name": "John", "age": 30, "car": str(i)}

            serialized_producer.produce_message(config.topic, message=message, key=message)
    finally:
        serialized_producer.shutdown()


def consume_serialized_messages():
    avro_deserializer = AvroDeserializer(message_schema_path=config.schema_file, key_schema_path=config.schema_file)

    consumer_config = {
        'bootstrap.servers': config.kafka_broker,
        'group.id': 'test_consumer',
        'auto.offset.reset': 'earliest',
        'value.deserializer': avro_deserializer,
    }

    consumer = KafkaDeserializingConsumer(topics=config.topic.split(), config=consumer_config)

    try:
        for message in consumer.consume_message():
            logger.info(f"Key: {message.key()}")
            logger.info(f"Message: {message.value()}")
    finally:
        consumer.shutdown()


def consume_manual_commit_messages():
    consumer_config = {
        'bootstrap.servers': config.kafka_broker,
        'group.id': 'test_consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    consumer = KafkaConsumer(topics=config.topic.split(), config=consumer_config)

    buffer = collections.deque(maxlen=5)

    try:
        for message in consumer.consume_message():
            logger.info(f"Key: {message.key()}")
            logger.info(f"Message: {message.value()}")

            buffer.append(message)

            if len(buffer) == 5:
                logger.debug(f"Checking buffer position 3, {buffer[2].value()}")

                consumer.consumer.commit(buffer[0], asynchronous=False)
    finally:
        consumer.shutdown()


if __name__ == "__main__":
    produce_messages()
    # produce_serialized_messages()
    # consume_messages()
    # consume_serialized_messages()
    consume_manual_commit_messages()
