package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import net.cakesolutions.kafka.akka.KafkaConsumerActor
import net.cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Records, Subscribe}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.AsyncAssertions
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random

object KafkaConsumerActorSpec {
  def kafkaProducer(kafkaPort: Int): KafkaProducer[String, String] =
    KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)

  def actorConf(topic: String): KafkaConsumerActor.Conf = {
    KafkaConsumerActor.Conf(List(topic))
  }
}

class KafkaConsumerActorSpec(system: ActorSystem) extends TestKit(system) with KafkaTestServer with ImplicitSender with AsyncAssertions {
  import KafkaConsumerActorSpec._

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
           | group.id = "test"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)
  }

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:${kafkaServer.kafkaPort}",
      groupId = "test",
      enableAutoCommit = false).witAutoOffsetReset(OffsetResetStrategy.EARLIEST)
  }

  def actorConfFromConfig(topic: String): KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(ConfigFactory.parseString(
      s"""
         | consumer.topics = ["$topic"]
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
        """.stripMargin)
    )

  def configuredActor(topic: String): Config = {
    ConfigFactory.parseString(
      s"""
         | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
         | group.id = "test"
         | enable.auto.commit = false
         | auto.offset.reset = "earliest"
         | consumer.topics = ["$topic"]
         | schedule.interval = 3000 milliseconds
         | unconfirmed.timeout = 3000 milliseconds
         | buffer.size = 8
        """.stripMargin)
  }

  "KafkaConsumerActors with different configuration types" should "consume a message successfully" in {

    (List(consumerConfFromConfig, consumerConf) zip List(actorConf(randomString(5)), actorConfFromConfig(randomString(5))))
      .foreach {
        case (consumerConfig, actorConf) =>
          val producer = kafkaProducer(kafkaServer.kafkaPort)
          producer.send(KafkaProducerRecord(actorConf.topics.head, None, "value"))
          producer.flush()

          val consumer = system.actorOf(KafkaConsumerActor.props(consumerConfig, actorConf, testActor))
          consumer ! Subscribe()

          expectMsgClass(30.seconds, classOf[Records[String, String]])
          consumer ! Confirm()
          expectNoMsg(5.seconds)
      }
  }

  "KafkaConsumerActor configured via props" should "consume a sequence of messages" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)

    val producer = kafkaProducer(kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    // Consumer and actor config in same config file
    val consumer = system.actorOf(KafkaConsumerActor.props(configuredActor(topic), new StringDeserializer(), new StringDeserializer(), testActor))
    consumer ! Subscribe()

    expectMsgClass(30.seconds, classOf[Records[String, String]])
    consumer ! Confirm()
    expectNoMsg(5.seconds)
  }

  //TODO changing actor config settings - timeout etc

  //TODO test message pattern

  //TODO review
  "KafkaConsumerActor in commit mode" should "consume a sequence of messages" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = kafkaProducer(kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), testActor))
    consumer ! Subscribe()

    val rec = expectMsgClass(30.seconds, classOf[Records[String, String]])
    consumer ! Confirm(Some(rec.offsets))
    expectNoMsg(5.seconds)
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
