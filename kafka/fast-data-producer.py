# Connect to any kafka broker
# Fetch stock price every second


import argparse
import logging
import confluent_kafka
import random
import time
import datetime
import atexit



# Logging configuaration
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger(__name__)
# - debug, info, warning, error
logger.setLevel(logging.DEBUG)


kafka_broker = "127.0.0.1:9092"     # default kafka broker location
topic = "stock-analyzer"            # default kafka topic to write to


def generate_data(producer, topic):
    logger.debug("Start to generate data")
    
    num_msg = 0
    start = time.time()
    while True:
        num_msg += 1
        price = random.randint(30, 120)
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%dT%H:%MZ")
        payload = ('[{"StockSymbol": "AAPL", "LastTradePrice": %d, "LastTradeDateTime": "%s"}]' % (price, timestamp)).encode('utf-8')
        producer.produce(topic, value=payload)
        producer.poll(0)

        # generate log for every 100,000 records
        if num_msg == 100000:
            end = time.time()
            logger.info("Write 100,000 records to Kafka server in %d seconds" % (end - start))
            start = end
            num_msg = 0
        

def shut_down(producer):
    try:   
        logger.info("Flushing pending messages to kafka, timeout is set to 10s")
        producer.flush()
        logger.info("Finish flushing pending messages to kafka")
    except Exception as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)


if __name__ == "__main__":
    # setup command-line argument
    parser = argparse.ArgumentParser()
    parser.add_argument("kafka_broker", help="The location of kafka broker")
    parser.add_argument("topic", help="The kafka topic to write to, such as stock-analyzer")

    args = parser.parse_args()
    topic = args.topic
    kafka_broker = args.kafka_broker


    # instantiate a kafka producer
    conf = {
        "bootstrap.servers": kafka_broker
    }
    producer = confluent_kafka.Producer(**conf)

    # Set up producer shutdown hook
    atexit.register(shut_down, producer)

    # Generate random data to kafka
    generate_data(producer, topic)








