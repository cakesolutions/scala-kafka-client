package cakesolutions.kafka

import java.util.Properties

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer, OffsetResetStrategy, ConsumerConfig}
import org.apache.kafka.common.serialization.Deserializer

object KafkaConsumer {

  object Conf {
    def apply[K, V](keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    bootstrapServers: String = "localhost:9092",
                    groupId: String = "test",
                    enableAutoCommit: Boolean = true,
                    autoCommitInterval: Int = 1000,
                    sessionTimeoutMs: Int = 30000): Conf[K, V] = {

      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toString)
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs.toString)
      apply(props, keyDeserializer, valueDeserializer)
    }

    def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Conf[K, V] =
      apply(config.toProperties, keyDeserializer, valueDeserializer)
  }

  case class Conf[K, V](props: Properties,
                        keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]) {

    def witAutoOffsetReset(strategy: OffsetResetStrategy): Conf[K,V] = {
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  strategy.toString)
      this
    }

    def withConf(config: Config): Conf[K, V] = {
      props.putAll(config.toProperties)
      this
    }
  }

  def apply[K, V](conf: Conf[K, V]): JKafkaConsumer[K, V] =
    new JKafkaConsumer[K, V](conf.props, conf.keyDeserializer, conf.valueDeserializer)
}
