package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import cakesolutions.kafka.akka.KafkaConsumerActor._
import cakesolutions.kafka.testkit.TestUtils
import cakesolutions.kafka.{KafkaConsumer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class KafkaConsumerActorRecoverySpec(system: ActorSystem) extends KafkaIntSpec(system) {

  import KafkaConsumerActorSpec._

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = "test",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }

  "KafkaConsumerActor with manual commit" should "recover to a commit point" in {

    val topic = TestUtils.randomString(5)

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), testActor))
    consumer ! Subscribe()

    val rec1 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec1.offsets.get(new TopicPartition(topic, 0)) shouldBe Some(1)

    //Commit the message
    consumer ! Confirm(rec1.offsets, commit = true)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec2.offsets.get(new TopicPartition(topic, 0)) shouldBe Some(2)

    //Message confirmed, but not commited
    consumer ! Confirm(rec2.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe

    // New subscription starts from commit point
    consumer ! Subscribe()
    val rec3 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec3.offsets.get(new TopicPartition(topic,0)) shouldBe Some(2)
    consumer ! Confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  "KafkaConsumerActor with self managed offsets" should "recover to a specified offset" in {
    val topic = TestUtils.randomString(5)

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), testActor))
    consumer ! Subscribe()

    val rec1 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec1.offsets.get(new TopicPartition(topic, 0)) shouldBe Some(1)

    //Stash the offsets for recovery, and confirm the message.
    val offsets = rec1.offsets
    consumer ! Confirm(offsets)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec2.offsets.get(new TopicPartition(topic, 0)) shouldBe Some(2)

    //Message confirmed
    consumer ! Confirm(rec2.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe

    // New subscription starts from specified offset
    consumer ! Subscribe(Some(offsets))
    val rec3 = expectMsgClass(30.seconds, classOf[Records[String, String]])
    rec3.offsets.get(new TopicPartition(topic,0)) shouldBe Some(2)
    consumer ! Confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }
}
