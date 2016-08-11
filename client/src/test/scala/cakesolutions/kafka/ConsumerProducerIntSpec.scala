package cakesolutions.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetResetStrategy}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class ConsumerProducerIntSpec extends KafkaIntSpec {

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val log = LoggerFactory.getLogger(getClass)

  val producerFromTypesafeConfig: KafkaProducer.Conf[String, String] = {
    KafkaProducer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
         """.stripMargin
      ), new StringSerializer, new StringSerializer
    )
  }

  val consumerFromTypesafeConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "$randomString"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)
  }

  val producerFromDirectConfig: KafkaProducer.Conf[String, String] = {
    KafkaProducer.Conf(new StringSerializer(),
      new StringSerializer(),
      bootstrapServers = s"localhost:$kafkaPort")
  }

  val consumerFromDirectConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false)
  }

  val consumerConfigWithEarliest: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }

  "KafkaConsumer and KafkaProducer from direct config" should "deliver and consume a message" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(producerFromDirectConfig)
    val consumer = KafkaConsumer(consumerFromDirectConfig)
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

  "KafkaConsumer and KafkaProducer from Typesafe config" should "deliver and consume a message" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(producerFromTypesafeConfig)
    val consumer = KafkaConsumer(consumerFromTypesafeConfig)

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    consumer.subscribe(List(topic))

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close()
    consumer.close()
  }

  "KafkaConsumer with earliest/latest config" should "receive all/no messages already on topic" in {
    val topic = randomString

    val producer = KafkaProducer(producerFromDirectConfig)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.send(KafkaProducerRecord(topic, Some("key"), "value2"))
    producer.flush()

    def consumeAndCount[K, V](conf: KafkaConsumer.Conf[K, V]): Int = {
      val consumer = KafkaConsumer(conf)
      consumer.subscribe(List(topic))

      val count = (1 to 30).map { _ =>
        consumer.poll(1000).count
      }.sum
      consumer.close()
      count
    }

    consumeAndCount(consumerFromDirectConfig) shouldEqual 0

    consumeAndCount(consumerConfigWithEarliest) shouldEqual 2

    producer.close()
  }
}
