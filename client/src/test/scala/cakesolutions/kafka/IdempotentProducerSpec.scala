package com.pirum.kafka

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random

class IdempotentProducerSpec extends KafkaIntSpec {
  private val log = LoggerFactory.getLogger(getClass)

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val idempotentProducerConfig: KafkaProducer.Conf[String, String] =
    KafkaProducer.Conf(
      new StringSerializer(),
      new StringSerializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      enableIdempotence = true
    )

  val transactionalProducerConfig: KafkaProducer.Conf[String, String] =
    KafkaProducer.Conf(
      new StringSerializer(),
      new StringSerializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      transactionalId = Some("t1"),
      enableIdempotence = true
    )

  val consumerConfig: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(
      new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false
    )

  val transactionConsumerConfig: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(
      new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false,
      isolationLevel = IsolationLevel.READ_COMMITTED
    )

  "Producer with idempotent config" should "deliver batch" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(idempotentProducerConfig)
    val consumer = KafkaConsumer(consumerConfig)

    consumer.subscribe(List(topic).asJava)

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

  "Producer with transaction" should "deliver batch" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(transactionalProducerConfig)
    val consumer = KafkaConsumer(transactionConsumerConfig)

    consumer.subscribe(List(topic).asJava)

    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)

    producer.initTransactions()

    try {
      producer.beginTransaction()
      producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
      producer.commitTransaction()
    } catch {
      case ex: KafkaException =>
        log.error(ex.getMessage, ex)
        producer.abortTransaction()
    }

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close()
    consumer.close()
  }
}
