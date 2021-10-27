package ca.mcit.bigdata.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object Kafka01Producer extends App {

    val topicName = "trips"

    val producerProperties = new Properties()
    producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](producerProperties)
    val dataSource = Source.fromFile("/users/valeria/STMdata/trips.txt")
    dataSource
      .getLines().slice(1, 51)
      .foreach(line => {
        producer.send(new ProducerRecord[String, String](topicName, line))
      })
    dataSource.close()
    producer.flush()
}
