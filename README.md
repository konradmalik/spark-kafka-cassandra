# spark-kafka-cassandra
This is an example/demo of Kafka-Spark Streaming-Cassandra interoperability, with Spark as a focal point.
Code is this repo shows how to efficiently perform the following workflow:
* read messages from Kafka into spark
* parse/perform basic operations on these messages
* save them to Cassandra

The clever Kafka producer wrapper KafkaSink provided several times faster processing than more naive usage. It was implemented accordig to this blogpost: https://allegro.tech/2015/08/spark-kafka-integration.html

### Folders structure
* Kafka-producer_consumer-example -> python scripts for light and easy Kafka Producer and Consumer implementations

During testing I used 3 docker containers (Kafka, Spark, Cassandra) to simulate a real world scenario.

Notes of commands to use:
'''bash

'''
