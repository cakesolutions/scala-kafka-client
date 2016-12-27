package cakesolutions.kafka.akka

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.TestProbe
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe.AutoPartition
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe, Unsubscribe}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord, KafkaTopicPartition}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.duration._
import scala.util.Random

object KafkaConsumerActorSpec {
  def kafkaProducer(kafkaHost: String, kafkaPort: Int): KafkaProducer[String, String] =
    KafkaProducer(KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = kafkaHost + ":" + kafkaPort))
}

class KafkaConsumerActorSpec(system_ : ActorSystem) extends KafkaIntSpec(system_) {

  import KafkaConsumerActorSpec._

  def this() = this(ActorSystem("KafkaConsumerActorSpec"))

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "$randomString"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)
  }

  def consumerConf: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST
    )

  def actorConfFromConfig: KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(ConfigFactory.parseString(
      s"""
         | schedule.interval = 1 second
         | unconfirmed.timeout = 3 seconds
         | max.redeliveries = 3
        """.stripMargin)
    )

  def configuredActor(topic: String): Config =
    ConfigFactory.parseString(
      s"""
         | bootstrap.servers = "localhost:$kafkaPort",
         | group.id = "$randomString"
         | enable.auto.commit = false
         | auto.offset.reset = "earliest"
         | schedule.interval = 1 second
         | unconfirmed.timeout = 3 seconds
         | max.redeliveries = 3
        """.stripMargin
    )

  "KafkaConsumerActors with different configuration types" should "each consume a message successfully" in {

    (List(consumerConfFromConfig, consumerConf) zip List(KafkaConsumerActor.Conf(), actorConfFromConfig))
      .foreach {
        case (consumerConfig, actorConf) =>
          val topic = randomString

          val producer = kafkaProducer("localhost", kafkaPort)
          producer.send(KafkaProducerRecord(topic, None, "value"))
          producer.flush()

          val consumer = KafkaConsumerActor(consumerConfig, actorConf, testActor)
          consumer.subscribe(AutoPartition(Seq(topic)))

          val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
          consumer.confirm(rs.offsets)
          expectNoMsg(5.seconds)

          consumer.unsubscribe()
          producer.close()
      }
  }

  "KafkaConsumerActor configured via props" should "consume a sequence of messages" in {
    val topic = randomString

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    // Consumer and actor config in same config file
    val consumer = system.actorOf(KafkaConsumerActor.props(configuredActor(topic), new StringDeserializer(), new StringDeserializer(), testActor))
    consumer ! Subscribe.AutoPartition(List(topic))

    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    consumer ! Confirm(rs.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  "KafkaConsumerActor configured via props" should "terminate itself if downstream actor terminates" in {
    val topic = randomString

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val testProbe = TestProbe()
    val downstreamActor = TestProbe().ref

    // Consumer and actor config in same config file
    val consumer = system.actorOf(KafkaConsumerActor.props(configuredActor(topic), new StringDeserializer(), new StringDeserializer(), downstreamActor))
    consumer ! Subscribe.AutoPartition(List(topic))

    // Initiate DeathWatch
    testProbe.watch(consumer)
    testProbe.watch(downstreamActor)

    downstreamActor ! PoisonPill
    testProbe.expectTerminated(downstreamActor)
    testProbe.expectTerminated(consumer)

    producer.close()
  }

  "KafkaConsumerActor configured in manual partition mode" should "consume a sequence of messages" in {
    val topic = randomString
    val topicPartition = KafkaTopicPartition(topic, 0)

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor))
    consumer ! Subscribe.ManualPartition(List(topicPartition))

    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    consumer ! Confirm(rs.offsets, commit = true)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  "KafkaConsumerActor configured in manual offset mode" should "consume a sequence of messages" in {
    val topic = randomString
    val offsets = Offsets(Map(KafkaTopicPartition(topic, 0) -> 0))

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor))
    consumer ! Subscribe.ManualOffset(offsets)

    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    consumer ! Confirm(rs.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  "KafkaConsumerActor configured in Auto Partition with manual offset mode" should "consume a sequence of messages" in {
    val topic = randomString

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor))
    consumer ! Subscribe.AutoPartitionWithManualOffset(Seq(topic),
      tps => Offsets(tps.map { tp => tp -> 0l }.toMap),
      _ => ())

    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    consumer ! Confirm(rs.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }
}