import os
import sys
import signal
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

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


def basic_consume_test(consumer_client, topics):
    try:
        consumer_client.subscribe(topics)
        while running:
            msg = consumer_client.poll(timeout=2.0)
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
                value = msg.value().decode('utf-8')
                print(value)
    finally:
        # Close down consumer to commit final offsets.
        consumer_client.close()


basic_consume_test(consumer, ['poems'])
