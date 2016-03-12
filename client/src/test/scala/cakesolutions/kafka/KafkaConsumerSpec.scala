package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import cakesolutions.kafka.testkit.TestUtils
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{OffsetResetStrategy, ConsumerRecords}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaConsumerSpec extends KafkaTestSpec {

  val log = LoggerFactory.getLogger(getClass)

  val typesafeConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "${TestUtils.randomString(5)}"
           | enable.auto.commit = false
        """.stripMargin), new StringDeserializer, new StringDeserializer)
  }

  val directConfig: KafkaConsumer.Conf[String, String] = {
    Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = s"localhost:$kafkaPort", enableAutoCommit = false)
  }

  val configWithEarliest: KafkaConsumer.Conf[String, String] = {
    Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = s"localhost:$kafkaPort", enableAutoCommit = false, autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }

  "KafkaConsumer with direct config" should "receive a message" in {
    val topic = TestUtils.randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    val consumer = KafkaConsumer(directConfig)
    consumer.subscribe(List(topic))

    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close()
    consumer.close()
  }

  "KafkaConsumer with typesafe config" should "receive a message" in {
    val topic = TestUtils.randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    val consumer = KafkaConsumer(typesafeConfig)
    consumer.subscribe(List(topic))

    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close()
    consumer.close()
  }

  "KafkaConsumer with earliest/latest config" should "receive all/no messages already on topic" in {
    val topic = TestUtils.randomString(5)

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.send(KafkaProducerRecord(topic, Some("key"), "value2"))
    producer.flush()

    def consumeAndCount[K, V](conf: KafkaConsumer.Conf[K, V]): Int = {
      val consumer = KafkaConsumer(conf)
      consumer.subscribe(List(topic))

      val count = (1 to 3).map { _ =>
        consumer.poll(1000).count()
      }
      consumer.close()

      count.sum
    }

    consumeAndCount(directConfig) shouldEqual 0

    consumeAndCount(configWithEarliest) shouldEqual 2

    producer.close
  }
}
