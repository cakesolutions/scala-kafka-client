package cakesolutions.kafka.akka

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Records, Subscribe}
import cakesolutions.kafka.testkit.TestUtils
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

class KafkaConsumerActorITSpec(system: ActorSystem)
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

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = "127.0.0.1:9092",
      groupId = "test",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  }

  def actorConf(topic: String): KafkaConsumerActor.Conf = {
    KafkaConsumerActor.Conf(List(topic))
  }

  "KafkaConsumerActor " should "Produce" in {
    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "127.0.0.1:9092")
    val topic = TestUtils.randomString(5)
    val receiver = system.actorOf(Props(classOf[ReceiverActor]))
    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf, actorConf(topic), receiver))

    1 to 100000 foreach { n =>
      producer.send(KafkaProducerRecord(topic, Some("key"), "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue" + n))
    }
    producer.flush()
    log.info("Delivered 100000 msg to topic {}", topic)
    consumer ! Subscribe()
    Thread.sleep(10000)

  }
}

object ReceiverActor {
}

class ReceiverActor extends Actor with ActorLogging {

  var total = 0
  var start = 0l

  override def receive: Receive = {
    case records: Records[_, _] =>

      if(total == 0)
        start = System.currentTimeMillis()

      //Type safe cast of records to correct serialisation type
      records.cast[String, String] match {
        case Some(r) =>
          total += r.records.count()
          log.info("!!!" + total)
          sender() ! Confirm(r.offsets)
          if(total >= 100000) {
            val totalTime = System.currentTimeMillis() - start
            log.info("Total Time: {}", totalTime)
            log.info("Msg per sec: {}", (100000 / totalTime * 100))
          }

        case None => log.warning("Received wrong Kafka records type!")
      }
  }
}
