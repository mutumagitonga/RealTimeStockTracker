import os
import sys
import json
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from dotenv import load_dotenv
import redis
import logging

# Logging module configuration
formatter = logging.Formatter("%(message)s")  # Set logger to print message only & remove logging level info
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[handler])

# Initialize redis client
redis_client = redis.Redis(host='localhost', port=6379)


def kafka_consumer_run():
    # Load the .env file here
    load_dotenv()

    # Access API key & secret from .env file
    confluent_api_key = os.environ.get("CONFLUENT_CLUSTER_API_KEY")
    confluent_api_secret_key = os.environ.get("CONFLUENT_CLUSTER_API_SECRET_KEY")

    # Define consumer client config
    consumer_config = {
        'bootstrap.servers': 'pkc-w8nyg.me-central1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '{}'.format(confluent_api_key),
        'sasl.password': '{}'.format(confluent_api_secret_key),
        'group.id': 'stock_price_group_test_1',
        'auto.offset.reset': 'earliest'
    }

    # Initialize Kafka consumer client...
    # ... as defined in https://docs.confluent.io/kafka-clients/python/current/overview.html
    consumer = Consumer(consumer_config)

    # Basic message consumption test
    topics = 'poems'

    try:
        consumer.subscribe([topics])
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error('%% %s [%d] reached end at offset %d\n' %
                                  (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                if isinstance(msg, Message):
                    print(msg)
                    # print(type(msg))
                    msg_val = msg.value()
                    print(msg_val)
                    # print(type(msg_val))
                    decoded_msg_val = msg_val.decode("utf-8")
                    print(f'Decoded: {decoded_msg_val}')
                    pythonified = json.loads(decoded_msg_val)
                    print(f'Python format: {pythonified}')
                    one_fell_swoop = json.loads(msg.value().decode("utf-8"))
                    print(f'ONCE: {one_fell_swoop}')
                    logging.info(f'LOGGER: {one_fell_swoop}')
                else:
                    print("Message not bytes or is None")
                # decoded_msg = json.loads(msg.decode('utf-8'))
                # decoded_msg = json.loads(msg.value)
                # print(decoded_msg)

                # logger.info(f"Received: {value}")
                # publish_msg(json.dumps(value))
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def publish_msg(val):
    # Publishing message to redis for real time viz
    redis_client.lpush('read_poems', val)
    redis_client.ltrim('read_poems', 0, 49)


if __name__ == "__main__":
    kafka_consumer_run()
