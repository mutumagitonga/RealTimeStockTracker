# RealTimeStockTracker
This application fetches real time stock prices from Yahoo Finance, publishes them to a confluent Kafka topic using a producer component. On the other end, a consumer component subscribes to the topics in the Kafka producer to access the real time updates. Finally, a simple web application is created using Flask to to display data in a browser. 
