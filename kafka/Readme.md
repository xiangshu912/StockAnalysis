# Kafka


## data-producer.py
use google finance api to fetch a stock price as kafka producer input, send to kafka server.

### Dependency
>kafka-python       https://github.com/dpkp/kafka-python

>googlefinance      https://pypi.python.org/pypi/googlefinance

>schedule           https://pypi.python.org/pypi/schedule

### Run
Run kafka in docker-machine(bigdata) with ip 192.168.99.100
```sh
python data-producer.py AAPL 192.168.99.100:9092 stock-analyzer
```


## fast-data-producer.py
use random value as kafka producer input, send large data to kafka server.

### Dependency
>kafka-python       https://github.com/dpkp/kafka-python

>confluent-kafka    https://github.com/confluentinc/confluent-kafka-python

### Run
Run kafka in docker-machine(bigdata) with ip 192.168.99.100
```sh
python fast-data-producer.py 192.168.99.100:9092 stock-analyzer
```