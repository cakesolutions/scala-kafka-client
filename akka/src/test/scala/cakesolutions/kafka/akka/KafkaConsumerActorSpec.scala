package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import cakesolutions.kafka.{KafkaProducer, KafkaProducerRecord, KafkaTestServer}
import com.typesafe.config.ConfigFactory
import net.cakesolutions.kafka.akka.KafkaConsumerActor.{Records, Subscribe}
import net.cakesolutions.kafka.akka.{KafkaActor, KafkaConsumerActor}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random

class KafkaConsumerActorSpec(system: ActorSystem) extends TestKit(system) with KafkaTestServer with ImplicitSender {
  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  def consumerConf(topic: String): KafkaConsumerActor.Conf[String, String] = {

    KafkaConsumerActor.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
           | group.id = "test"
           | key.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
           | value.deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
        """.stripMargin),
      List(topic)
    )
  }

  "KafkaConsumerActor in self managed offsets mode" should "consume a sequence of messages" in {

    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info(s"Using topic [$topic] and kafka port [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)

    //    val probe = TestProbe()
    val consumer = system.actorOf(KafkaActor.consumer(consumerConf(topic), testActor), "consumer")

    producer.send(KafkaProducerRecord(topic, None, "value"))
//    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    //    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()
    log.info("ready to poll")
    //    consumer ! Reset(Offsets(Map(TopicPartition(), 0)))
    consumer ! Subscribe()
    Thread.sleep(5000)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    Thread.sleep(5000)
    producer.send(KafkaProducerRecord(topic, None, "value"))

    implicit val timeout:FiniteDuration = 120.seconds
    expectMsgClass(timeout, classOf[Records[String, String]])
  }

  "KafkaConsumerActor in commit mode" should "consume a sequence of messages" in {

  }

  //  ConfigFactory.parseString(
  //    """
  //      |akka.actor.deployment {
  //      |  /amqpProducer/workers {
  //      |    router = round-robin-pool
  //      |    nr-of-instances = 5
  //      |  }
  //      |}
  //      |
  //    """.stripMargin)

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
