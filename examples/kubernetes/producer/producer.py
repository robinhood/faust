import json
from random import random
from kafka import KafkaProducer

TOPIC = 'test'
KEY = 'score'


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(ex)


def connect_kafka_producer():
    _producer = None
    try:
        # host.docker.internal is how a docker container connects to the local
        # machine.
        # Don't use in production, this only works with Docker for Mac in
        # development
        _producer = KafkaProducer(
            bootstrap_servers=['host.docker.internal:9092'],
            api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(ex)
    finally:
        return _producer


if __name__ == '__main__':
    kafka_producer = connect_kafka_producer()
    for index in range(0, 10000):
        message = {
            'index': index,
            'value': round(random(), 2),
        }
        publish_message(kafka_producer, TOPIC, KEY, json.dumps(message))
    if kafka_producer is not None:
        kafka_producer.close()
