package cakesolutions.kafka

import java.lang.management.ManagementFactory

import cakesolutions.kafka.testkit.TestUtils
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class KafkaHealthSpec extends KafkaIntSpec {

  val log = LoggerFactory.getLogger(getClass)

  val producerFromDirectConfig: KafkaProducer.Conf[String, String] = {
    KafkaProducer.Conf(new StringSerializer(),
      new StringSerializer(),
      bootstrapServers = "localhost:" + kafkaPort)
  }

  val consumerFromDirectConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = TestUtils.randomString(5),
      enableAutoCommit = false)
  }

  "KafkaConsumer Health Check" should "report correct health" in {
    val topic = TestUtils.randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(producerFromDirectConfig)
    val consumer = KafkaConsumer(consumerFromDirectConfig)
    consumer.subscribe(List(topic))

    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    getProducerHealth.status shouldEqual Health.Ok
    getConsumerHealth.status shouldEqual Health.Ok

    val records2: ConsumerRecords[String, String] = consumer.poll(1000)
    records2.count() shouldEqual 1

    getProducerHealth.status shouldEqual Health.Ok
    getConsumerHealth.status shouldEqual Health.Ok

    kafkaServer.close()

    consumer.poll(0)

    getProducerHealth.status shouldEqual Health.Critical
    getConsumerHealth.status shouldEqual Health.Critical

    producer.close()
    consumer.close()
  }

  def consumerHealth = KafkaHealth.kafkaConsumerHealth(ManagementFactory.getPlatformMBeanServer, 1, 0)
  def producerHealth = KafkaHealth.kafkaProducerHealth(ManagementFactory.getPlatformMBeanServer, 1, 0)

  def getConsumerHealth: Health = consumerHealth.getHealth
  def getProducerHealth: Health = producerHealth.getHealth
}
