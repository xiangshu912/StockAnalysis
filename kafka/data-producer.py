# Connect to any kafka broker
# Fetch stock price every second


import argparse
import logging
import json
import time
import datetime
import schedule
import atexit
import random

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from googlefinance import getQuotes




# Logging configuaration
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger("data-producer")
# - debug, info, warning, error
logger.setLevel(logging.DEBUG)


symbol = "AAPL"
kafka_broker = "127.0.0.1:9092"     # default kafka broker location
topic = "stock-analyzer"            # default kafka topic to write to


def fetch_price(producer, symbol):
    try:     
        logger.debug("Start to fetch price for %s" % symbol)
        price = random.randint(30, 120)
        payload = ('[{"StockSymbol":"AAPL","LastTradePrice":%d,"LastTradeDateTime":"%s"}]' % (price, timestamp)).encode('utf-8')
        
        producer.send(topic=topic, value=payload, timestamp_ms=time.time())
        logger.debug("Sent stock price for %s, price is %s" % (symbol, price))

    except KafkaTimeoutError as timeout_error:
        logger.warn("Failed to send stock price for %s to kafka, caused by: %s", (symbol, timeout_error.message))
    
    except Exception:
        logger.warn("Failed to fetch stock price for %s", symbol)


def shut_down(producer):
    try:   
        logger.info("Flushing pending messages to kafka, timeout is set to 10s")
        producer.flush(10)
        logger.info("Finish flushing pending messages to kafka")
    except KafkaError as kafka_error:
        logger.warn("Failed to flush pending messages to kafka, caused by: %s", kafka_error.message)
    finally:
        try:
            logger.info("Closing kafka connection")
            producer.close()
        except Exception as e:
            logger.warn("Failed to close kafka connection, caused by: %s", e.message)
            


if __name__ == "__main__":
    # setup command-line argument
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", help="The stock symbol, such as AAPL")
    parser.add_argument("kafka_broker", help="The location of kafka broker")
    parser.add_argument("topic", help="The kafka topic to write to")

    args = parser.parse_args()
    symbol = args.symbol
    topic = args.topic
    kafka_broker = args.kafka_broker


    # instantiate a kafka producer
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    schedule.every(1).second.do(fetch_price, producer, symbol)
    
    atexit.register(shut_down, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)










