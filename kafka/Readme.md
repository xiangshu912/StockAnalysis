# Kafka


## simple-data-producer.py
use random number as kafka producer input, send to kafka server.

### Dependency
kafka-python    https://github.com/dpkp/kafka-python
schedule        https://pypi.python.org/pypi/schedule

### Run
Run kafka in docker-machine(bigdata) with ip 192.168.99.100
```sh
python data-producer.py AAPL 192.168.99.100:9092 stock-analyzer
```