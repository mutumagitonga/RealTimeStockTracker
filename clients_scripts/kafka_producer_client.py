import json
import os
import time

import pandas as pd
import logging

import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv

# Configure logging module specifically for this module
logger = logging.getLogger(__name__)

# Load the API key & API secret from the .env file using load_dotenv() method
load_dotenv()

# Get API key & secret key
confluent_api_key = os.environ.get("CONFLUENT_CLUSTER_API_KEY")
confluent_api_secret_key = os.environ.get("CONFLUENT_CLUSTER_API_SECRET_KEY")

# Define producer configuration
producer_config = {
    'bootstrap.servers': 'pkc-w8nyg.me-central1.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '{}'.format(confluent_api_key),
    'sasl.password': '{}'.format(confluent_api_secret_key),
}
# Initialize Kafka producer client...
# ... as defined in https://docs.confluent.io/kafka-clients/python/current/overview.html
producer = Producer(producer_config)


def get_stock_info(symbol):
    try:
        # Fetch ticker info
        ticker_info = yf.download(symbol, period='1d')
        # print(f'{symbol}: \n{ticker_info}')
        # print(type(ticker_info))
        with open("./datafiles/test_parent.csv", "w") as test_parent:
            ticker_info.to_csv(test_parent)

        df = pd.read_csv('./datafiles/test_parent.csv')
        # print(f"Date: {df['Date'][0]} Type: {type(df.Date)}")
        # print(f"Price: {df['Close'][0]} Type: {type(df.Close)}")
        date = df['Date'][0]
        price = df['Close'][0]

        return date, price
    except Exception as e:
        logger.error(f'Unsuccessful: {e}')


def kafka_producer_run(symbols, topic):
    i = 0
    while True:
        for sym in symbols:
            # MULTIPLE TICKERS DOWNLOAD
            try:
                date, price = get_stock_info(sym)

                if price is not None:
                    # Format message in JSON
                    message = {'symbol': sym, 'date': date, 'price': price}
                    # Send message to kafka
                    try:
                        producer.produce(topic, key=sym, value=json.dumps(message))
                        producer.flush()
                        logger.info(f'Sent: {message}')
                    except Exception as e:
                        logger.error(f'Error: {e}')
            except TypeError:
                logger.error(f'No stock information available for {sym}')
        i += 1
        time.sleep(20)


if __name__ == "__main__":
    # stock_tickers = ['MSFT', 'GOOGL', 'AMZN', 'TSLA']
    stock_tickers = ['CSCO', 'WMT', 'PANW', 'TM']
    kafka_topic = 'poems'
    kafka_producer_run(stock_tickers, kafka_topic)
