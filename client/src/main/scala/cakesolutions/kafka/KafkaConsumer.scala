package cakesolutions.kafka

import java.util.Properties

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer

object KafkaConsumer {
  def apply[K, V](props: Properties): JKafkaConsumer[K, V] =
    new JKafkaConsumer[K, V](props)

  def apply[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): JKafkaConsumer[K, V] =
    new JKafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  def apply[K, V](keyDeserializer: Deserializer[K],
                  valueDeserializer: Deserializer[V],
                  bootstrapServers: String = "localhost:9092",
                  groupId: String = "test",
                  enableAutoCommit: Boolean = true,
                  autoCommitInterval: Int = 1000,
                  sessionTimeoutMs: Int = 30000): JKafkaConsumer[K, V] = {

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", enableAutoCommit.toString)
    props.put("auto.commit.interval.ms", autoCommitInterval.toString)
    props.put("session.timeout.ms", sessionTimeoutMs.toString)
    apply(props, keyDeserializer, valueDeserializer)
  }

  def apply[K, V](config: Config): JKafkaConsumer[K, V] =
    apply(config.toProperties)
}
