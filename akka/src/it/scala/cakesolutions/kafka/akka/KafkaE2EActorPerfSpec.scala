package cakesolutions.kafka.akka

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActor.AutoPilot
import akka.testkit.{ImplicitSender, TestActor, TestKit, TestProbe}
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import cakesolutions.kafka.akka.KafkaConsumerActor.Subscribe.AutoPartition
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.Promise
import scala.util.Random

/**
  * Ad hoc performance test for validating async consumer performance.  Pass environment variable KAFKA with contact point for
  * Kafka server e.g. -DKAFKA=127.0.0.1:9092
  */
class KafkaE2EActorPerfSpec(system_ : ActorSystem)
  extends TestKit(system_)
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  override implicit val patienceConfig = PatienceConfig(Span(10L, Seconds), Span(100L, Millis))

  val config = ConfigFactory.load()

  val msg1k = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/1k.txt")).mkString

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(config.getConfig("consumer"),
      new StringDeserializer,
      new StringDeserializer
    )
  }

  def consumerActorConf: KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(config.getConfig("consumer"))

  val producerConf = KafkaProducer.Conf(config.getConfig("producer"), new StringSerializer, new StringSerializer)

  "KafkaConsumerActor to KafkaProducer with async commit" should "perform" in {
    val sourceTopic = randomString
    val targetTopic = randomString
    val totalMessages = 100000

    //For loading the source topic with test data
    val testProducer = KafkaProducer[String, String](producerConf)

    val producer = system.actorOf(KafkaProducerActor.props(producerConf))

    val pilot = new PipelinePilot(producer, targetTopic, totalMessages)
    val receiver = TestProbe()
    receiver.setAutoPilot(pilot)

    val consumer = KafkaConsumerActor(consumerConf, consumerActorConf, receiver.ref)

    1 to totalMessages foreach { n =>
      testProducer.send(KafkaProducerRecord(sourceTopic, None, msg1k))
    }
    testProducer.flush()
    log.info("Delivered {} messages to topic {}", totalMessages, sourceTopic)

    consumer.subscribe(AutoPartition(Seq(sourceTopic)))

    whenReady(pilot.future) { case (totalTime, messagesPerSec) =>
      log.info("Total Time millis : {}", totalTime)
      log.info("Messages per sec  : {}", messagesPerSec)

      totalTime should be < 7000L

      consumer.unsubscribe()
      testProducer.close()
      log.info("Done")
    }
  }
}

class PipelinePilot(producer: ActorRef, targetTopic: String, expectedMessages: Long) extends TestActor.AutoPilot {

  private val log = LoggerFactory.getLogger(getClass)

  private var total = 0
  private var start = 0l

  private val finished = Promise[(Long, Long)]()

  def future = finished.future

  val matcher = ConsumerRecords.extractor[String, String]

  override def run(sender: ActorRef, msg: Any): AutoPilot = {

    if (total == 0)
      start = System.currentTimeMillis()

    matcher.unapply(msg) match {
      case Some(r) =>
        total += r.size

        producer.tell(
          msg = ProducerRecords(r.toProducerRecords(targetTopic), Some(Confirm(r.offsets, commit = true))),
          sender = sender
        )

        if (total >= expectedMessages) {
          val totalTime = System.currentTimeMillis() - start
          val messagesPerSec = expectedMessages / totalTime * 1000
          finished.success((totalTime, messagesPerSec))
          TestActor.NoAutoPilot
        } else {
          TestActor.KeepRunning
        }

      case None =>
        log.warn("Received unknown messages!")
        TestActor.KeepRunning
    }
  }
}
