package com.pirum.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import com.pirum.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}

import scala.util.Random

class KafkaProducerActorSpec(system_ : ActorSystem)
    extends KafkaIntSpec(system_) {

  def this() = this(ActorSystem("KafkaProducerActorSpec"))

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val deserializer = new StringDeserializer
  val consumerConf = KafkaConsumer.Conf(
    deserializer,
    deserializer,
    bootstrapServers = s"localhost:$kafkaPort",
    groupId = "test",
    enableAutoCommit = false,
    autoOffsetReset = OffsetResetStrategy.EARLIEST
  )

  val serializer = new StringSerializer
  val producerConf = KafkaProducer.Conf(
    serializer,
    serializer,
    bootstrapServers = s"localhost:$kafkaPort"
  )

  "KafkaProducerActor" should "write a given batch to Kafka" in {
    val topic = randomString
    val probe = TestProbe()
    val producer = system.actorOf(KafkaProducerActor.props(producerConf))
    val batch: Seq[ProducerRecord[String, String]] = Seq(
      KafkaProducerRecord(topic, "foo"),
      KafkaProducerRecord(topic, "key", "value"),
      KafkaProducerRecord(topic, "bar")
    )
    val message = ProducerRecords(batch, Some('response))

    probe.send(producer, message)

    probe.expectMsg('response)

    val results = consumeFromTopic(topic, 3, 10000)

    results(0) shouldEqual ((None, "foo"))
    results(1) shouldEqual ((Some("key"), "value"))
    results(2) shouldEqual ((None, "bar"))
  }

  "KafkaProducerActor" should "write a given batch to Kafka, requiring no response" in {
    import scala.concurrent.duration._

    val topic = randomString
    val probe = TestProbe()
    val producer = system.actorOf(KafkaProducerActor.props(producerConf))
    val batch: Seq[ProducerRecord[String, String]] = Seq(
      KafkaProducerRecord(topic, "foo"),
      KafkaProducerRecord(topic, "key", "value"),
      KafkaProducerRecord(topic, "bar")
    )
    val message = ProducerRecords(batch)

    probe.send(producer, message)

    probe.expectNoMessage(3.seconds)

    val results = consumeFromTopic(topic, 3, 10000)

    results(0) shouldEqual ((None, "foo"))
    results(1) shouldEqual ((Some("key"), "value"))
    results(2) shouldEqual ((None, "bar"))
  }

  private def consumeFromTopic(
      topic: String,
      expectedNumOfMessages: Int,
      timeout: Long
  ) =
    kafkaServer.consume(
      topic,
      expectedNumOfMessages,
      timeout,
      deserializer,
      deserializer
    )
}
