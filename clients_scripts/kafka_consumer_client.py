import os
import json
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from dotenv import load_dotenv
import redis
import logging



# # Logging module configuration
# formatter = logging.Formatter("%(message)s")  # Set logger to print message only & remove logging level info
# handler = logging.StreamHandler()
# handler.setFormatter(formatter)
# logging.basicConfig(level=logging.INFO, handlers=[handler])

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
            msg = consumer.poll(timeout=1.0)
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
                    # print(msg)
                    decoded_msg = json.loads(msg.value().decode("utf-8"))
                    print(f'symbol: {decoded_msg["symbol"]} | price: {decoded_msg["price"]}')
                    # logging.info(f' LOGGER: {decoded_msg}')
                    # Publish message to redis
                    # publish_msg(decoded_msg)
                else:
                    print("Message not bytes or is None")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def publish_msg(val):
    # Publish the stock prices to Redis for real time viz
    redis_client.lpush('stock_monitor', val)
    redis_client.ltrim('stock_monitor', 0, 49)  # Keep only the 50 latest messages


if __name__ == "__main__":
    kafka_consumer_run()
