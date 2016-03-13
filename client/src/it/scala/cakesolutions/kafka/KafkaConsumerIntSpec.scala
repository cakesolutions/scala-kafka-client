package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import cakesolutions.kafka.testkit.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaConsumerIntSpec extends FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  "Kafka client" should "receive" in {

    val consumer = KafkaConsumer(
      Conf(new StringDeserializer(),
        new StringDeserializer(),
        bootstrapServers = "192.168.99.100:9092",
        groupId = "test",
        enableAutoCommit = false,
        autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )

    consumer.subscribe(List("test2"))
    log.info("!!!")
    Thread.sleep(5000)
    1 to 10000 foreach { _ =>
      val r = consumer.poll(10000)
      log.info("Received {} records", r.count())
    }

    consumer.close
  }

  "Kafka client consuming from a non-existent topic" should "receive when topic creatd by producer and a message delivered " in {
    //    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info(s"Using topic [$topic] and kafka port []")

    Thread.sleep(10000)
    val consumer = KafkaConsumer(
      Conf(
      new StringDeserializer(),
        new StringDeserializer(),
        bootstrapServers = "192.168.99.100:9092",
        groupId = TestUtils.randomString(5),
        autoOffsetReset = OffsetResetStrategy.EARLIEST)
    )

    consumer.subscribe(List(topic))
    val records1 = consumer.poll(1000)

    records1.count() shouldEqual 0

    //    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    //    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "192.168.99.100:9092")
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.flush()

    while (true) {
      val records2: ConsumerRecords[String, String] = consumer.poll(10000)
      log.info(":" + records2.count())
      //      records2.count() shouldEqual 1
    }

    //    producer.close
    consumer.close
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
