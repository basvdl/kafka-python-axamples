import os
from pathlib import Path

log_level = os.getenv('LOG_LEVEL', 'DEBUG')

kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
topic = os.getenv('TOPIC', 'test')
schema_file = Path('/Users/bas/code/baslus/kafka/schema.avsc')
