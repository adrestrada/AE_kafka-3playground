package ca.mcit.bigdata.kafka

import org.apache.kafka.clients.producer.ProducerConfig.{BOOTSTRAP_SERVERS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object Kafka03ProduceEnrichedTopic {
  def ProduceEnrichedTopic (enrichedList : List[EnrichedTrip]) : Unit = {
    val producerProperties = new Properties()
    producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](producerProperties)

    val topicName = "enrichedTrips_Adriana"
    for (line <-enrichedList) {
      val csvLine = EnrichedTrip.toCsv(line)
      producer.send(new ProducerRecord[String, String](topicName,csvLine))
    }
    producer.flush()
  }
}
