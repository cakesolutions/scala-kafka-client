package cakesolutions.kafka

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.scalatest.concurrent.Waiters.Waiter
import scala.concurrent.ExecutionContext.Implicits.global
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaConsumerSpec extends KafkaIntSpec {

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val log = LoggerFactory.getLogger(getClass)

  val serializer = (msg: String) => msg.getBytes
  val deserializer = (bytes: Array[Byte]) => new String(bytes)

  val consumerConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(KafkaDeserializer(deserializer),
      KafkaDeserializer(deserializer),
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false)
  }

  val producerConfig: KafkaProducer.Conf[String, String] = {
    KafkaProducer.Conf(KafkaSerializer(serializer),
      KafkaSerializer(serializer),
      bootstrapServers = s"localhost:$kafkaPort")
  }

  "KafkaConsumer and KafkaProducer with Function serializers" should "deliver and consume a message" in {
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(producerConfig)
    val consumer = KafkaConsumer(consumerConfig)
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

  "Kafka producer with bad serializer" should "return a failed future" in {
    val w = new Waiter
    val topic = randomString
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val badSerializer = (msg: String) => {
      throw new Exception("Serialization failed")
    }

    val producerConfig = KafkaProducer.Conf(KafkaSerializer(serializer),
      KafkaSerializer(badSerializer),
      bootstrapServers = s"localhost:$kafkaPort")
    val producer = KafkaProducer(producerConfig)

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    val future = producer.send(KafkaProducerRecord(topic, Some("key"), "value"))

    future.onFailure {
      case e: Exception => w.dismiss()
    }

    w.await()
    producer.close()
  }
}
