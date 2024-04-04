import os
from confluent_kafka import Consumer
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
    'sasl.username': 'AM4S2AAL2PRP74TB',
    'sasl.password': 'N6mFpXJ5GPybcwICRK/ex4dUorcKi6+M3lHBFlMpYdoPhh/CjGbQH87k23EzmBws',
    'group.id': 'stock_price_group_test_1',
    'auto.offset.reset': 'earliest'
})

