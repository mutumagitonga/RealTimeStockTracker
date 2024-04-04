import json
import os
import pandas as pd

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

# # Try publishing a simple message to the poems topic as a test
# try:
#     # producer.produce('poems', value="First message sent from python producer client application")
#     producer.produce('poems', key="12", value="Another message sent from python client with key")
#     producer.flush()
# except Exception as e:
#     print(e)


def fetch_and_produce_stock_price(symbol):
    try:
        stock = yf.Ticker(symbol)
        all_stock_info_json = stock.info
        with open("msft_stock.json", "w") as stocks_file:
            json.dump(all_stock_info_json, stocks_file)

        price_5days = stock.history(period='5d')
        with open("msft_stock_5d.csv", "w") as stocks_file_5d:
            price_5days.to_csv(stocks_file_5d)

        price = stock.history(period='5d')['Close'].iloc[-1]
        message = f'{symbol}:{float(f"{price:.2f}")}'  # Combine symbol and price
        print(message)

        df = pd.read_csv("msft_stock_5d.csv")
        date = df['Date'].iloc[-1].split(" ", 1)[0]
        print(date)

        producer.produce('stocks', key=date, value=message)
        producer.flush()  # Ensure the message is sent immediately
        print(f'Sent {date} ticker:price(message): {message}')
    except Exception as e:
        print(f'Error sending data: {e}')


fetch_and_produce_stock_price('MSFT')
