import os

import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load the API key & API secret from the .env file using load_dotenv() method
load_dotenv()

# Get API key & secret key
confluent_api_key = os.environ.get("CONFLUENT_CLUSTER_API_KEY")
confluent_api_secret_key = os.environ.get("CONFLUENT_CLUSTER_API_SECRET_KEY")


# Initialize Kafka producer...
# ... as defined in https://docs.confluent.io/kafka-clients/python/current/overview.html
producer = Producer({
    'bootstrap.servers': 'pkc-w8nyg.me-central1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '{}'.format(confluent_api_key),
    'sasl.password': '{}'.format(confluent_api_secret_key),
})

# Try publishing a simple message to the poems topic as a test
try:
    # producer.produce('poems', value="First message sent from python producer client application")
    producer.produce('poems', key="12", value="Another message sent from python client with key")
    producer.flush()
except Exception as e:
    print(e)

