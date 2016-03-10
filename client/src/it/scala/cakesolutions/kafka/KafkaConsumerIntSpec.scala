package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Random

class KafkaConsumerIntSpec extends FlatSpecLike
with Matchers
with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  "Kafka client" should "receive" in {

    val consumer = KafkaConsumer(Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "192.168.99.100:9092", enableAutoCommit = false).
      witAutoOffsetReset(OffsetResetStrategy.EARLIEST))

    consumer.subscribe(List("test2"))
    log.info("!!!")
    Thread.sleep(5000)
    1 to 10000 foreach { _ =>
      val r = consumer.poll(10000)
      log.info("Received {} records", r.count())
    }

    consumer.close
  }

  val random = new Random()

  def randomString(length: Int): String =
    random.alphanumeric.take(length).mkString
}
