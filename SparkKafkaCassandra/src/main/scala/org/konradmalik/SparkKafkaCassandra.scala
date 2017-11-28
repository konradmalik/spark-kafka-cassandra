package org.konradmalik

import java.{lang, util}

import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSONArray


object SparkKafkaCassandra {

  final val keyspace: String = "testkeyspace"
  final val tableName1: String = "testtable1"
  final val tableName1Mapping: SomeColumns = SomeColumns("timestamp", "key", "value")
  final val tableName2: String = "testtable2"
  final val tableName2Mapping: SomeColumns = SomeColumns("timestamp", "key", "value")
  // streaming
  private final val streamingInterval: Int = 2; //seconds
  // kafka
  private final val kafkaBrokers: String = "127.0.0.1:9092"
  // incoming topic
  private final val topics: Array[String] = Array("tospark")
  // outcoming topic
  private final val outtopic: String = "fromspark"

  // cassandra
  private final val contactPoint: String = "127.0.0.1"
  private final val user: String = "cassandra"
  private final val password: String = "cassandra"

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val sparkConf = new SparkConf()
      .setAppName("Spark Kafka Streaming")
      .setMaster("local[*]")
      .set("spark.executor.instances", "3")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculation", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //cassandra
      .set("spark.cassandra.connection.host", contactPoint)
      .set("spark.cassandra.auth.username", user)
      .set("spark.cassandra.auth.password", password)
      // streaming
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(streamingInterval))
    // configure the streaming context to remember the RDDs produced
    // choose at least 2x the time of the streaming interval
    ssc.remember(Seconds(4))

    val consumerProps = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "group.id" -> "my-group",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> "true"
    )

    val producerProps = new util.HashMap[String, Object]()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer])


    // KAFKA SINK
    val kafkaSink: Broadcast[KafkaSink] = sc.broadcast(KafkaSink(producerProps))

    // stream
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, consumerProps))

    // prepare cassandra
    CassandraConnector(sc).withSessionDo { session =>
      session.execute(s"create KEYSPACE if not exists " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = 'false'")
      session.execute(s"DROP TABLE IF EXISTS " + keyspace + "." + tableName1)
      session.execute(s"DROP TABLE IF EXISTS " + keyspace + "." + tableName2)
      session.execute(s"CREATE TABLE IF NOT EXISTS " + keyspace + "." + tableName1 + " (key TEXT, value DOUBLE,PRIMARY KEY(key))")
      session.execute(s"CREATE TABLE IF NOT EXISTS " + keyspace + "." + tableName2 + " (key TEXT, value DOUBLE,PRIMARY KEY(key))")
    }

    // firstly map stream, in this example multiply values 10 times
    val mappedStream: DStream[(String, Double)] = stream.map(record => {
      val newValue = record.value().toDouble * 10

      record.key() -> newValue
    })

    // now for each RDD
    mappedStream.foreachRDD(rdd => {

      // CASSANDRA CONNECTOR
      val cassandra: CassandraConnector = CassandraConnector.apply(sc)

      rdd.foreach(entry => {
        logger.info("-----------------------------------------------------------------------------------")
        logger.info("record key: " + entry._1)
        logger.info("record value: " + entry._2)

        // do something, here we take sqrt of our newValue, and we want it to be saved in separate table
        // but sent together with the old value using kafka
        val root: Double = Math.sqrt(entry._2)

        // send further as JSON
        val json: _root_.scala.util.parsing.json.JSONArray = JSONArray(List(entry._2, root))
        kafkaSink.value.send(outtopic, entry._1, json.toString())

        // save to tables separately
        cassandra.withSessionDo(session => {
          // old value
          val insertString1: String = s"INSERT INTO $keyspace.$tableName1 (key,value) " + "VALUES (?,?)"
          val preparedStatement1: PreparedStatement = session.prepare(insertString1)
          val boundStatement1: BoundStatement = preparedStatement1.bind(entry._1, new lang.Double(entry._2))
          session.executeAsync(boundStatement1)

          // root
          val insertString2: String = s"INSERT INTO $keyspace.$tableName2 (key,value) " + "VALUES (?,?)"
          val preparedStatement2: PreparedStatement = session.prepare(insertString2)
          val boundStatement2: BoundStatement = preparedStatement2.bind(entry._1, new lang.Double(root))
          session.executeAsync(boundStatement2)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}