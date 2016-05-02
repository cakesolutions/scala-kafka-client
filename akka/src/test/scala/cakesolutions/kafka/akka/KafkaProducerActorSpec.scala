package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.TestProbe
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class KafkaProducerActorSpec(system_ : ActorSystem) extends KafkaIntSpec(system_) {

  def this() = this(ActorSystem("KafkaProducerActorSpec"))

  val log = LoggerFactory.getLogger(getClass)

  val deserializer = new StringDeserializer
  val consumerConf = KafkaConsumer.Conf(
    deserializer, deserializer,
    bootstrapServers = s"localhost:$kafkaPort",
    groupId = "test",
    enableAutoCommit = false,
    autoOffsetReset = OffsetResetStrategy.EARLIEST
  )

  val serializer = new StringSerializer
  val producerConf = KafkaProducer.Conf(serializer, serializer, bootstrapServers = s"localhost:$kafkaPort")

  "KafkaProducerActor" should "write given batch to Kafka" in {
    val topic = "sometopic"
    val probe = TestProbe()
    val producer = system.actorOf(KafkaProducerActor.props(producerConf))
    val batch: Seq[ProducerRecord[String, String]] = Seq(
      KafkaProducerRecord(topic, "foo"),
      KafkaProducerRecord(topic, "key", "value"),
      KafkaProducerRecord(topic, "bar"))
    val message = KafkaIngestible(batch, Some('response))

    probe.send(producer, message)

    probe.expectMsg('response)

    val results = consumeFromTopic(topic, 3, 10000)

    results(0) shouldEqual (None, "foo")
    results(1) shouldEqual (Some("key"), "value")
    results(2) shouldEqual (None, "bar")
  }

  private def consumeFromTopic(topic: String, expectedNumOfMessages: Int, timeout: Long) = {
    val consumer = KafkaConsumer(consumerConf)
    consumer.subscribe(List(topic))

    var total = 0
    var collected = Vector.empty[(Option[String], String)]
    val start = System.currentTimeMillis()

    while (total <= expectedNumOfMessages && System.currentTimeMillis() < start + timeout) {
      val records = consumer.poll(100)
      collected = collected ++ records.map(r => (Option(r.key()), r.value()))
      total += records.count()
    }

    consumer.close()

    if (collected.size < expectedNumOfMessages) {
      sys.error(s"Did not receive expected amount messages. Expected $expectedNumOfMessages but got ${collected.size}.")
    }

    collected
  }
}
