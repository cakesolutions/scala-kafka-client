package cakesolutions.kafka

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

import scala.util.Random

class ExactlyOnceStreamSpec extends KafkaIntSpec {
  private val log = LoggerFactory.getLogger(getClass)

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val transactionalProducerConfig: KafkaProducer.Conf[String, String] =
    KafkaProducer.Conf(new StringSerializer(),
      new StringSerializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      transactionalId = Some("T1"))

  val transactionalConsumerConfig: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false)

  "KC" should "t" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(transactionalProducerConfig)
    producer.initTransactions()

    val consumer = KafkaConsumer(transactionalConsumerConfig)

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
}
