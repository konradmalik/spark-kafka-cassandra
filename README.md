# spark-kafka-cassandra
This is an example/demo of Kafka - Spark Streaming - Cassandra/Kafka interoperability, with Spark as a focal point.
Code is this repo shows how to efficiently perform the following workflow:
* read messages from Kafka into spark
* parse/perform some operations on these messages, which result in multiple versions of data
* save some to Cassandra tables
* concatenate the rest and send it again to Kafka, from there it can go for example to a dashboard for live plotting etc.


The clever Kafka producer wrapper KafkaSink provided several times faster processing than more naive usage. It was implemented accordig to this blogpost: https://allegro.tech/2015/08/spark-kafka-integration.html

## Folders structure
* *Kafka-producer_consumer-example* -> python scripts for light and easy Kafka Producer and Consumer implementations.
  **Please note** that you need to have "kafka-python" package installed. The easiest way to do this is to install from conda-forge it in an Anaconda environment.
* *SparkKafkaCassandra* -> scala project conaining Spark Streaming code

## Usage
During testing we can use 2 docker containers (Kafka, Cassandra) and Spark Streaming running on my host machine.
Also note, that focuses on implementation, handling serialization error, efficient producer/cassandra session handling etc. Thus, the messeges are just constant numbers for simplicity and cassandra table is poorly defined (the one and only row is constatntly overwritten).

1. Kafka

  This will run dockerized Kafka locally (first launch):
  ```bash
  $ docker run -p 2181:2181 -p 9092:9092 \
    --name kafka \
    --env ADVERTISED_HOST=127.0.0.1 \
    --env ADVERTISED_PORT=9092 \
    spotify/kafka
  ```
  Further launches:
  ```bash
  $ docker start kafka -a
  ```
2. Cassandra

  First run:
  ```bash
  $ docker run -p 9042:9042 --name cassandra cassandra:latest
  ```
  Further launches:
  ```bash
  $ docker start cassandra -a
  ```
3. Spark Streaming:

  Go to *SparkKafkaCassandra* dir and:
  clean and compile using maven:
  ```bash
  $ mvn clean && mvn package
  ```
4. Launch all (after starting docker images):

  Producer:
  ```bash
  $ python Kafka-producer_consumer-example/producer.py
  ```
  Consumer:
  ```bash
  $ python Kafka-producer_consumer-example/consumer.py
  ```
  Spark streaming:
  ```bash
  $ java -jar target/SparkKafkaCassandra-0.0.1-SNAPSHOT-jar-with-dependencies.jar
  ```
