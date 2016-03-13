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
                    groupId: String,
                    enableAutoCommit: Boolean = true,
                    autoCommitInterval: Int = 1000,
                    sessionTimeoutMs: Int = 30000,
                    maxPartitionFetchBytes: String = 262144.toString,
                    autoOffsetReset: OffsetResetStrategy = OffsetResetStrategy.LATEST): Conf[K, V] = {

      val configMap = Map[String, AnyRef](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableAutoCommit.toString,
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> autoCommitInterval.toString,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> sessionTimeoutMs.toString,
      ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> maxPartitionFetchBytes,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.toString.toLowerCase)

      apply(configMap, keyDeserializer, valueDeserializer)
    }

    def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Conf[K, V] =
      apply(config.toPropertyMap, keyDeserializer, valueDeserializer)
  }

  /**
    * Configuration object for the KafkaConsumer.  Key and Value serialisers are provided explicitly.
    * @param props Map of KafkaConsumer Properties.  Usually created via the Object helpers.
    * @tparam K Key Serializer type
    * @tparam V Value Serializer type
    */
  case class Conf[K, V](props: Map[String, AnyRef],
                        keyDeserializer: Deserializer[K],
                        valueDeserializer: Deserializer[V]) {

    /**
      * With additional config defined in supplied Typesafe config.  Supplied config overrides existing properties
      * @param config Typesafe Config
      */
    def withConf(config: Config): Conf[K, V] = {
      copy(props = props ++ config.toPropertyMap)
    }

    /**
      * @return True if configured to Auto Commit Mode.
      */
    def isAutoCommitMode: Boolean = {
      props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).getOrElse("").toString.equals("true")
    }

    /**
      * Returns Conf with additional property.
      */
    def withProperty(key: String, value: AnyRef) = {
      copy(props = props + (key -> value))
    }
  }

  /**
    * Create a Java KafkaConsumer client with config provided in Conf class.
    * @param conf Configuration for the consumer
    * @tparam K Key Serialiser type
    * @tparam V Value Serialiser type
    * @return KafkaConsumer Client
    */
  def apply[K, V](conf: Conf[K, V]): JKafkaConsumer[K, V] = {
    new JKafkaConsumer[K, V](conf.props, conf.keyDeserializer, conf.valueDeserializer)
  }
}
