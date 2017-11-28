# Producer.py
from kafka import KafkaProducer
from kafka.errors import KafkaError
from random import *
import time

producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                        key_serializer=str.encode,
                        value_serializer=str.encode)

while True:
    # Asynchronous by default
    future = producer.send('tospark', key='key1', value='1')

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)

    # wait
    time.sleep(1)
