import os
import sys
import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
import redis

# Initialize redis client
redis_client = redis.Redis(host='localhost', port=6379)


def kafka_consumer_run():

    # Load the .env file here
    load_dotenv()

    # Access API key & secret from .env file
    confluent_api_key = os.environ.get("CONFLUENT_CLUSTER_API_KEY")
    confluent_api_secret_key = os.environ.get("CONFLUENT_CLUSTER_API_SECRET_KEY")

    # Initialize Kafka consumer...
    # ... as defined in https://docs.confluent.io/kafka-clients/python/current/overview.html
    consumer = Consumer({
        'bootstrap.servers': 'pkc-w8nyg.me-central1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': '{}'.format(confluent_api_key),
        'sasl.password': '{}'.format(confluent_api_secret_key),
        'group.id': 'stock_price_group_test_1',
        'auto.offset.reset': 'earliest'
    })

    # Basic message consumption test
    running = True
    topics = 'poems'

    try:
        consumer.subscribe([topics])
        while running:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                value = json.loads(msg.value().decode('utf-8'))
                print(f"New: {value}")
                publish_msg(json.dumps(value))
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def publish_msg(val):
    # Publishing message to redis for real time viz
    redis_client.lpush('poems', val)


if __name__ == "__main__":
    kafka_consumer_run()