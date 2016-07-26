package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe.AutoPartition
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe, Unsubscribe}
import cakesolutions.kafka.testkit.TestUtils
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord, KafkaTopicPartition}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object KafkaConsumerActorSpec {
  def kafkaProducer(kafkaHost: String, kafkaPort: Int): KafkaProducer[String, String] =
    KafkaProducer(KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = kafkaHost + ":" + kafkaPort))
}

class KafkaConsumerActorSpec(system_ : ActorSystem) extends KafkaIntSpec(system_) {

  import KafkaConsumerActorSpec._

  def this() = this(ActorSystem("KafkaConsumerActorSpec"))

  val log = LoggerFactory.getLogger(getClass)

  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "${TestUtils.randomString(5)}"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)
  }

  def consumerConf: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = TestUtils.randomString(5),
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST
    )

  def actorConfFromConfig: KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(ConfigFactory.parseString(
      s"""
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
        """.stripMargin)
    )

  def configuredActor(topic: String): Config =
    ConfigFactory.parseString(
      s"""
         | bootstrap.servers = "localhost:$kafkaPort",
         | group.id = "${TestUtils.randomString(5)}"
         | enable.auto.commit = false
         | auto.offset.reset = "earliest"
         | topics = ["$topic"]
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
        """.  stripMargin
    )

  "KafkaConsumerActors with different configuration types" should "each consume a message successfully" in {

    (List(consumerConfFromConfig, consumerConf) zip List(KafkaConsumerActor.Conf(), actorConfFromConfig))
      .foreach {
        case (consumerConfig, actorConf) =>
          val topic = TestUtils.randomString(5)

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
    val topic = TestUtils.randomString(5)

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

  "KafkaConsumerActor configured in manual partition mode" should "consume a sequence of messages" in {
    val topic = TestUtils.randomString(5)
    val topicPartition = KafkaTopicPartition(topic, 0)

    val producer = kafkaProducer("localhost", kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    // Consumer and actor config in same config file
    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor))
    consumer ! Subscribe.ManualPartition(List(topicPartition))

    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    consumer ! Confirm(rs.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }
}