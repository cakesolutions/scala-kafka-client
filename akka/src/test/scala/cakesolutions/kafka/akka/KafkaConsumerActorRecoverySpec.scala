package cakesolutions.kafka.akka

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe.{ManualOffset, ManualPartition}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe, TriggerConsumerFailure, Unsubscribe}
import cakesolutions.kafka.testkit.TestUtils
import cakesolutions.kafka.{KafkaConsumer, KafkaProducerRecord, KafkaTopicPartition}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class KafkaConsumerActorRecoverySpec(_system: ActorSystem) extends KafkaIntSpec(_system) {

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

  private def randomTopicPartition = KafkaTopicPartition(TestUtils.randomString(5), 0)

  "KafkaConsumerActor with manual commit" should "recover to a commit point on resubscription" in {
    val topicPartition = randomTopicPartition
    val subscription = Subscribe.AutoPartition(List(topicPartition.topic()))

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val consumer = KafkaConsumerActor(consumerConf, KafkaConsumerActor.Conf(), testActor)
    consumer.subscribe(subscription)

    val rec1 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec1.offsets.get(topicPartition) shouldBe Some(1)

    //Commit the message
    consumer.confirm(rec1.offsets, commit = true)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec2.offsets.get(topicPartition) shouldBe Some(2)

    //Message confirmed, but not commited
    consumer.confirm(rec2.offsets)
    expectNoMsg(5.seconds)

    consumer.unsubscribe()
    consumer.subscribe(subscription)

    // New subscription starts from commit point
    val rec3 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec3.offsets.get(topicPartition) shouldBe Some(2)
    consumer.confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer.unsubscribe()
    producer.close()
  }

  it should "recover to a commit point on consumer failure" in {
    val topicPartition = randomTopicPartition
    val subscription = Subscribe.AutoPartition(List(topicPartition.topic()))

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val consumer = KafkaConsumerActor(consumerConf, KafkaConsumerActor.Conf(), testActor)
    consumer.subscribe(subscription)

    val rec1 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec1.offsets.get(topicPartition) shouldBe Some(1)

    // Commit the message
    consumer.confirm(rec1.offsets, commit = true)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec2.offsets.get(topicPartition) shouldBe Some(2)

    // Reset subscription
    consumer.ref ! TriggerConsumerFailure

    // New subscription starts from commit point
    val rec3 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec3.offsets.get(topicPartition) shouldBe Some(2)
    consumer.confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer.unsubscribe()
    producer.close()
  }

  "KafkaConsumerActor with self managed offsets" should "recover to a specified offset on resubscription" in {
    val topicPartition = randomTopicPartition

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val consumer = KafkaConsumerActor(consumerConf, KafkaConsumerActor.Conf(), testActor)
    consumer.subscribe(ManualPartition(List(topicPartition)))

    val rec1 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec1.offsets.get(topicPartition) shouldBe Some(1)

    // Stash the offsets for recovery, and confirm the message.
    val offsets = rec1.offsets
    consumer.confirm(offsets)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec2.offsets.get(topicPartition) shouldBe Some(2)

    //Message confirmed
    consumer.confirm(rec2.offsets)
    expectNoMsg(5.seconds)

    // Reset subscription
    consumer.unsubscribe()
    consumer.subscribe(ManualOffset(offsets))

    // New subscription starts from specified offset
    val rec3 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec3.offsets.get(topicPartition) shouldBe Some(2)
    consumer.confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer.unsubscribe()
    producer.close()
  }

  it should "recover to a specified offset on consumer failure" in {
    val topicPartition = randomTopicPartition

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val consumer = supervisedActor(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor))
    consumer ! Subscribe.ManualPartition(List(topicPartition))

    val rec1 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec1.offsets.get(topicPartition) shouldBe Some(1)

    // Stash the offsets for recovery, and confirm the message.
    val offsets = rec1.offsets
    consumer ! Confirm(offsets)
    expectNoMsg(5.seconds)

    producer.send(KafkaProducerRecord(topicPartition.topic(), None, "value"))
    producer.flush()

    val rec2 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec2.offsets.get(topicPartition) shouldBe Some(2)

    // Reset subscription
    consumer ! TriggerConsumerFailure

    // New subscription starts from specified offset
    val rec3 = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    rec3.offsets.get(topicPartition) shouldBe Some(2)
    consumer ! Confirm(rec3.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  private def supervisedActor(props: Props): ActorRef = system.actorOf(ConsumerSupervisorActor.props(props))
}

private object ConsumerSupervisorActor {
  def props(childProps: Props): Props = Props(new ConsumerSupervisorActor(childProps))
}

private class ConsumerSupervisorActor(childProps: Props) extends Actor with ActorLogging {

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10) {
    case _: KafkaConsumerActor.ConsumerException =>
      log.info("Consumer exception caught. Restarting consumer.")
      SupervisorStrategy.Restart
    case _ =>
      SupervisorStrategy.Escalate
  }

  private val child = context.actorOf(childProps, "child")

  override def receive: Receive = {
    case m => child.forward(m)
  }
}