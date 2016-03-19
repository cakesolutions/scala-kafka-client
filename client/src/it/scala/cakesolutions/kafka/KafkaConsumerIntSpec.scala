package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import cakesolutions.kafka.testkit.TestUtils
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class KafkaConsumerIntSpec extends FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  val consumer = KafkaConsumer(
    Conf(new StringDeserializer(),
      new StringDeserializer(),
      bootstrapServers = "127.0.0.1:9092",
      groupId = "test",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
  )

  "Kafka Consumer" should "perform" in {
    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "127.0.0.1:9092")
    val topic = TestUtils.randomString(5)

    1 to 100000 foreach { n =>
      producer.send(KafkaProducerRecord(topic, Some("key"), "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue" + n))
    }
    log.info("Delivered 100000 msg to topic {}", topic)

    producer.flush()

    consumer.subscribe(List(topic))

    var start = 0l

    var total = 0

    while (total < 100000) {
      if(total == 0)
        start = System.currentTimeMillis()
      val count = consumer.poll(1000).count()
      total += count
    }

    val totalTime = System.currentTimeMillis() - start
    log.info("Total Time: {}", totalTime)
    log.info("Msg per sec: {}", 100000 / totalTime * 100 )

    consumer.close()
    producer.close()
  }
}
