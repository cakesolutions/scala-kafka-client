package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe, Unsubscribe}
import cakesolutions.kafka.testkit.TestUtils
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

object KafkaConsumerActorSpec {
  def kafkaProducer(kafkaHost: String, kafkaPort: Int): KafkaProducer[String, String] =
    KafkaProducer(KafkaProducer.Conf(new StringSerializer(), new StringSerializer(), bootstrapServers = kafkaHost + ":" + kafkaPort))

  def actorConf(topic: String): KafkaConsumerActor.Conf = {
    import Retry._
    KafkaConsumerActor.Conf(List(topic), retryStrategy = Strategy(Interval.Linear(1.seconds), Logic.FiniteTimes(10)))
  }
}

class KafkaConsumerActorSpec(system: ActorSystem) extends KafkaIntSpec(system) {

  import KafkaConsumerActorSpec._

  def this() = this(ActorSystem("MySpec"))

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

  def consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = TestUtils.randomString(5),
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }

  def actorConfFromConfig(topic: String): KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(ConfigFactory.parseString(
      s"""
         | consumer.topics = ["$topic"]
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
         | retryStrategy {
         |   interval {
         |     type = "linear"
         |     duration = 1000
         |   }
         |   logic {
         |     type = finiteTimes
         |     retryCount = 10
         |   }
         | }
        """.stripMargin)
    )

  def configuredActor(topic: String): Config = {
    ConfigFactory.parseString(
      s"""
         | bootstrap.servers = "localhost:$kafkaPort",
         | group.id = "${TestUtils.randomString(5)}"
         | enable.auto.commit = false
         | auto.offset.reset = "earliest"
         | consumer.topics = ["$topic"]
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
         | retryStrategy {
         |   interval {
         |     type = "linear"
         |     duration = 1000
         |   }
         |   logic {
         |     type = finiteTimes
         |     retryCount = 10
         |   }
         | }
        """.stripMargin)
  }

  "KafkaConsumerActors with different configuration types" should "consume a message successfully" in {

    (List(consumerConfFromConfig, consumerConf) zip List(actorConf(TestUtils.randomString(5)), actorConfFromConfig(TestUtils.randomString(5))))
      .foreach {
        case (consumerConfig, actorConf) =>
          val producer = kafkaProducer("localhost", kafkaPort)
          producer.send(KafkaProducerRecord(actorConf.topics.head, None, "value"))
          producer.flush()

          val consumer = system.actorOf(KafkaConsumerActor.props(consumerConfig, actorConf, testActor))
          consumer ! Subscribe()

          val rs = expectMsgClass(30.seconds, classOf[KeyValuesWithOffsets[String, String]])
          consumer ! Confirm(rs.offsets)
          expectNoMsg(5.seconds)

          consumer ! Unsubscribe
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
    consumer ! Subscribe()

    val rs = expectMsgClass(30.seconds, classOf[KeyValuesWithOffsets[String, String]])
    consumer ! Confirm(rs.offsets)
    expectNoMsg(5.seconds)

    consumer ! Unsubscribe
    producer.close()
  }

  //TODO changing actor config settings - timeout etc

  //TODO test message pattern
}