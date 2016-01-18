package cakesolutions.kafka

import java.util.Properties

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.{Logger, LoggerFactory}

object KafkaConsumer {
  def apply[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): KafkaConsumer[K, V] =
    new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  def apply[K, V](keyDeserializer: Deserializer[K],
                  valueDeserializer: Deserializer[V],
                  bootstrapServers: String = "localhost:9092",
                  groupId: String = "test",
                  enableAutoCommit: Boolean = true,
                  autoCommitInterval: Int = 1000,
                  sessionTimeoutMs: Int = 30000): KafkaConsumer[K, V] = {

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", enableAutoCommit.toString)
    props.put("auto.commit.interval.ms", autoCommitInterval.toString)
    props.put("session.timeout.ms", sessionTimeoutMs.toString)
    apply(props, keyDeserializer, valueDeserializer)
  }

  def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): KafkaConsumer[K, V] =
    apply(config.toProperties, keyDeserializer, valueDeserializer)
}

class KafkaConsumer[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {

  import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer}

  import scala.collection.JavaConversions._

  private val log: Logger = LoggerFactory.getLogger(getClass)

  val consumer = new JKafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  def subscribe(topics: List[String]): Unit = consumer.subscribe(topics)

  def consume(timeout: Long): ConsumerRecords[K, V]  = {
    val records = consumer.poll(timeout)
    log.info("Got [{}]", records.count())
    records
  }

  def close() = {
    log.debug("Closing consumer")
    consumer.close()
  }
}