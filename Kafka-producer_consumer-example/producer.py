# Producer.py

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load the rest of the env variables

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    # Asynchronous by default
    future = producer.send('testtopic', b'raw_bytes')

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset) 
