package cakesolutions.kafka

import cakesolutions.kafka.KafkaConsumer.Conf
import cakesolutions.kafka.testkit.TestUtils
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Ad hoc performance test for validating consumer performance.  Pass environment variable KAFKA with contact point for
  * Kafka server e.g. -DKAFKA=127.0.0.1:9092
  */
class KafkaConsumerPerfSpec extends FlatSpecLike
  with Matchers
  with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  val config = ConfigFactory.load()

  val msg1k = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/1k.txt")).mkString

  val consumer = KafkaConsumer(
    Conf(config.getConfig("consumer"),
      new StringDeserializer,
      new StringDeserializer)
  )

  "Kafka Consumer with single partition topic" should "perform" in {
    val topic = TestUtils.randomString(5)
    val producer = KafkaProducer[String, String](config.getConfig("producer"))

    1 to 100000 foreach { n =>
      producer.send(KafkaProducerRecord(topic, None, msg1k))
    }
    producer.flush()
    log.info("Delivered 100000 msg to topic {}", topic)

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
