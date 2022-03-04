package com.pirum.kafka

import com.pirum.kafka.KafkaConsumer.Conf
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.util.Random

/** Ad hoc performance test for validating consumer performance.  Pass environment variable KAFKA with contact point for
  * Kafka server e.g. -DKAFKA=127.0.0.1:9092
  */
class KafkaConsumerPerfSpec
    extends AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val log = LoggerFactory.getLogger(getClass)

  val config = ConfigFactory.load()

  val msg1k = scala.io.Source
    .fromInputStream(getClass.getResourceAsStream("/1k.txt"))
    .mkString

  val consumer = KafkaConsumer(
    Conf(
      config.getConfig("consumer"),
      new StringDeserializer,
      new StringDeserializer
    )
  )

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  "Kafka Consumer with single partition topic" should "perform" in {
    val topic = randomString
    val producerConf = KafkaProducer.Conf(
      config.getConfig("producer"),
      new StringSerializer,
      new StringSerializer
    )
    val producer = KafkaProducer[String, String](producerConf)

    1 to 100000 foreach { _ =>
      producer.send(KafkaProducerRecord(topic, None, msg1k))
    }
    producer.flush()
    log.info("Delivered 100000 msg to topic {}", topic)

    consumer.subscribe(List(topic).asJava)

    var start = 0L

    var total = 0

    while (total < 100000) {
      if (total == 0)
        start = System.currentTimeMillis()
      val count = consumer.poll(1000).count()
      total += count
    }

    val totalTime = System.currentTimeMillis() - start
    val messagesPerSec = 100000 / totalTime * 1000
    log.info("Total Time millis : {}", totalTime)
    log.info("Messages per sec  : {}", messagesPerSec)

    totalTime should be < 4000L

    consumer.close()
    producer.close()
  }
}
