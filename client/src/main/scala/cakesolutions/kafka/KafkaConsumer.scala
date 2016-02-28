package cakesolutions.kafka

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer => JKafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.common.serialization.Deserializer
import scala.collection.JavaConversions._

object KafkaConsumer {

  object Conf {
    def apply[K, V](keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    bootstrapServers: String = "localhost:9092",
                    groupId: String = "test",
                    enableAutoCommit: Boolean = true,
                    autoCommitInterval: Int = 1000,
                    sessionTimeoutMs: Int = 30000): Conf[K, V] = {

      val configMap = Map[String, AnyRef](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableAutoCommit.toString,
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> autoCommitInterval.toString,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> sessionTimeoutMs.toString)

      apply(configMap, keyDeserializer, valueDeserializer)
    }

    def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Conf[K, V] =
      apply(config.toPropertyMap, keyDeserializer, valueDeserializer)
  }

  case class Conf[K, V](props: Map[String, AnyRef],
                        keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]) {

    /**
     * Sets the OffsetResetStrategy.  One of: LATEST, EARLIEST, NONE
     */
    def witAutoOffsetReset(strategy: OffsetResetStrategy): Conf[K, V] = {
      this.copy(props = props + (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> strategy.toString.toLowerCase))
    }

    def withConf(config: Config): Conf[K, V] = {
      copy(props = props ++ config.toPropertyMap)
    }

    def isAutoCommitMode: Boolean = {
      props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).equals("true")
    }
  }

  def apply[K, V](conf: Conf[K, V]): JKafkaConsumer[K, V] = {
    new JKafkaConsumer[K, V](conf.props, conf.keyDeserializer, conf.valueDeserializer)
  }
}
