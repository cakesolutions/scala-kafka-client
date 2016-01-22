package cakesolutions.kafka

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaIntSpec extends KafkaTestServer {

  val log = LoggerFactory.getLogger(getClass)

  "Ka" should "test" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info("Using topic {}", topic)
    log.info("ZK:" + kafkaServer.zookeeperConnect)
    log.info("!!:" + kafkaServer)
    log.info("Kafka Port: [{}]", kafkaPort)

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)

    val consumer = KafkaConsumer(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:" + kafkaPort)
    consumer.subscribe(List(topic))
    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("1"), "a"))
    producer.flush()

    val records = consumer.poll(1000)

    records.count() shouldEqual 1
    producer.close
    consumer.close
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
