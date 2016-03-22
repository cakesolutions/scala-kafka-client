package cakesolutions.kafka.akka

import akka.actor.{Props, ActorLogging, Actor, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Records, Subscribe}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class KafkaConsumerActorRebalanceSpec(system: ActorSystem)
  extends TestKit(system)
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with AsyncAssertions {

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val config = ConfigFactory.load()

  val msg1k = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/1k.txt")).mkString

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(config.getConfig("consumer"),
      new StringDeserializer,
      new StringDeserializer
    )
  }

  def actorConf(topic: String): KafkaConsumerActor.Conf = {
    KafkaConsumerActor.Conf(List(topic)).withConf(config.getConfig("consumer"))
  }

  "KafkaConsumerActor" should "rebalance without failing" in {
    import scala.concurrent.ExecutionContext.Implicits.global

    val topic = "multiPartitionTopic"
    val producer = KafkaProducer[String, String](config.getConfig("producer"))

    val receiver = system.actorOf(Props(classOf[ReceiverActor]))

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), receiver))

    Future {
      1 to 100000 foreach { n =>
        producer.send(KafkaProducerRecord(topic, Some("" + n), msg1k))
        Thread.sleep(500)
      }
    }
    log.info("!!!!")
    consumer ! Subscribe()
    Thread.sleep(1000000)
    log.info("Done")
  }
}

class ReceiverActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case records: Records[_, _] =>
      //Type safe cast of records to correct serialisation type
      records.cast[String, String] match {
        case Some(r) =>
          log.info("!:" + r.records.count())
          Thread.sleep(35000)
          sender() ! Confirm(r.offsets, commit = true)

        case None => log.warning("Received wrong Kafka records type!")
      }
  }
}

