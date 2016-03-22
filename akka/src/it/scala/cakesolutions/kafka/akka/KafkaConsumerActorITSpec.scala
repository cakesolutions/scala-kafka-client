package cakesolutions.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Random

class KafkaConsumerActorITSpec(system: ActorSystem)
  extends TestKit(system)
    with ImplicitSender
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with AsyncAssertions {

  import KafkaConsumerActorSpec._

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = "192.168.99.100:9092",
      groupId = "test",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }
  "KafkaConsumerActor " should "Produce" in {
    val producer = kafkaProducer("192.168.99.100", 9092)

    1 to 200000 foreach { n =>
      producer.send(KafkaProducerRecord("test2", n.toString, "value sdkfl ;asdf ;sdlkf ;aslkdfj salkdf j;askld fj;aklsdj f;klasj df;kls jdfasfs" +
        "lskdjf ;klasj df;klasjd;fklj as;dfkjsa;dfklj " +
        "as;dfklj ;askdj f;asklj f;lskdj f;aksj df;kasjdf;kl jsdf" +
        "sldfj ;aslkdj f;ksj df;klsj f;ksdj ;fkjasd;fk" +
        "slkd fj;sakjdf;lks j" +
        "sdfajfsdjf;lksjdf;klsja;fkljsd;fk"))
    }
    producer.flush()
    log.info("1234567")

  }

  "KafkaConsumerActor" should "consume a sequence of messages" in {
    val topic = "test2"

    //    val producer = kafkaProducer("192.168.99.100", 9092)
    //
    //    1 to 20 foreach { n =>
    //      producer.send(KafkaProducerRecord(topic, n.toString, "value"))
    //    }
    //    producer.flush()
    //    log.info("1234567")
    //    Thread.sleep(20000)

    val probe = TestProbe()(system)

    // Consumer and actor config in same config file
    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), probe.ref))
    consumer ! Subscribe()

    var cnt = 0
    1 to 10000 foreach { _ =>
      val r = probe.expectMsgClass(30.seconds, classOf[KeyValuesWithOffsets[String, String]])
      log.info("Received {} records", r.records.count())
      cnt += r.records.count()
      // Thread.sleep(5000)
      consumer ! Confirm(r.offsets)
    }
    log.info("Total {} records", cnt)

    consumer ! Unsubscribe
    //    producer.close()
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
