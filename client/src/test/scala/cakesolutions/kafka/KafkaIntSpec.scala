package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaIntSpec extends KafkaTestServer {

  val log = LoggerFactory.getLogger(getClass)

  "Kafka client" should "send and receive" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    val consumer = KafkaConsumer(Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:" + kafkaPort))
    consumer.subscribe(List(topic))

    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close
    consumer.close
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
