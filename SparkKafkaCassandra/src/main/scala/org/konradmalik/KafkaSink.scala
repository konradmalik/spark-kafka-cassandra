package org.konradmalik

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

// https://allegro.tech/2015/08/spark-kafka-integration.html

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, key: String, value: String): Unit = producer.send(new ProducerRecord(topic, key, value))
}

object KafkaSink {
  def apply(config: java.util.HashMap[String, Object]): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}