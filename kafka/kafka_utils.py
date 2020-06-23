import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.cimpl import KafkaException, Consumer, Message, Producer

logger = logging.getLogger(__file__)


class Kafka(ABC):
    @abstractmethod
    def shutdown(self):
        pass


class KafkaProducer(Kafka):
    def __init__(self, config: Dict):
        self.producer = self._init_producer(config)

    @staticmethod
    def _delivery_report(error, message):
        if error is not None:
            logger.error(f"Message delivery failed: {error}")
        else:
            logger.debug(
                f"Message delivered to topic `{message.topic()}`, partition `{message.partition()}`. "
                f"Payload: {message.value()}"
            )

    @staticmethod
    def _init_producer(config: Dict) -> Producer:
        """config must contain:
            'bootstrap.servers'
        but may contain every other kafka setting as well
        """
        assert "bootstrap.servers" in config.keys()
        return Producer(config)

    def produce_message(self, topic: str, message: Dict, key: Optional[str] = None, **produce_args):
        try:
            self.producer.produce(
                topic,
                value=message,
                key=key,
                callback=self._delivery_report,
                **produce_args,
            )
        except KafkaException as e:
            logger.error(f"KafkaException: {e}")
        except BufferError:
            logger.error(f"The internal producer message queue is full")
            self.producer.poll(1)
            self.producer.produce(
                topic,
                value=message,
                callback=self._delivery_report,
                **produce_args,
            )

    def shutdown(self):
        logger.info(f"Shutdown producer, flushing buffer")
        self.producer.flush()


class KafkaSerializingProducer(KafkaProducer):
    @staticmethod
    def _init_producer(config: Dict) -> SerializingProducer:
        """config must contain:
            'bootstrap.servers'
            'value.serializer'
        but may contain every other kafka setting as well
        """
        assert "bootstrap.servers" in config.keys()
        assert "value.serializer" in config.keys()
        return SerializingProducer(config)

    def produce_message(self, topic: str, message: Dict, key: Optional[Dict] = None, **produce_args):
        try:
            self.producer.produce(
                topic,
                value=message,
                key=key,
                on_delivery=self._delivery_report,
                **produce_args,
            )
        except KafkaException as e:
            logger.error(f"KafkaException: {e}")
        except BufferError:
            logger.error(f"The internal producer message queue is full")
            self.producer.poll(1)
            self.producer.produce(
                topic,
                value=message,
                on_delivery=self._delivery_report,
                **produce_args,
            )


class KafkaConsumer(Kafka):
    def __init__(self, topics: List[str], config: Dict):
        self.consumer = self._init_consumer(topics, config)

    @staticmethod
    def _init_consumer(topics: List[str], config: Dict) -> Consumer:
        """config must contain:
            `bootstrap.servers`
            'group.id'
        but may contain every other kafka setting as well
        """
        assert "bootstrap.servers" in config.keys()
        assert "group.id" in config.keys()
        consumer = Consumer(config)
        consumer.subscribe(topics)
        return consumer

    def consume_message(self, timeout: Optional[float] = 1.0):
        while True:
            message: Message = self.consumer.poll(timeout)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                yield message

    def shutdown(self):
        logger.info(f"Shutdown consumer")
        self.consumer.close()


class KafkaDeserializingConsumer(KafkaConsumer):
    @staticmethod
    def _init_consumer(topics: List[str], config: Dict) -> Consumer:
        """config must contain:
            `bootstrap.servers`
            'group.id'
        but may contain every other kafka setting as well
        """
        assert "bootstrap.servers" in config.keys()
        assert "group.id" in config.keys()
        consumer = DeserializingConsumer(config)
        consumer.subscribe(topics)
        return consumer
