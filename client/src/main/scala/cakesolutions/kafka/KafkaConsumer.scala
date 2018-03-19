package cakesolutions.kafka

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy, KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/**
  * Utilities for creating a Kafka consumer.
  *
  * This singleton object provides tools for creating Kafka consumers using helpful functions.
  * Unlike with [[KafkaProducer]], the consumer object is not wrapped in an object that provides a Scala-like API.
  */
object KafkaConsumer {

  /**
    * Implicit conversion to support calling the org.apache.kafka.clients.consumer.KafkaConsumer.offsetsForTimes method with a Map[TopicPartition, scala.Long].
    */
  implicit def toJavaOffsetQuery(offsetQuery: Map[TopicPartition, scala.Long]): java.util.Map[TopicPartition, java.lang.Long] =
    offsetQuery
      .map { case (tp, time) => tp -> new java.lang.Long(time) }
      .asJava

  /**
    * Utilities for creating Kafka consumer configurations.
    */
  object Conf {

    /**
      * Kafka consumer configuration constructor with common configurations as parameters.
      * For more detailed configuration, use the other [[Conf]] constructors.
      *
      * @param keyDeserializer deserializer for the key
      * @param valueDeserializer deserializer for the value
      * @param bootstrapServers a list of host/port pairs to use for establishing the initial connection to the Kafka cluster
      * @param groupId a unique string that identifies the consumer group this consumer belongs to
      * @param enableAutoCommit if true the consumer's offsets will be periodically committed in the background
      * @param autoCommitInterval the frequency in milliseconds that the consumer offsets are auto-committed to Kafka when auto commit is enabled
      * @param sessionTimeoutMs the timeout used to detect failures when using Kafka's group management facilities
      * @param maxPartitionFetchBytes the maximum amount of data per-partition the server will return
      * @param maxPollRecords the maximum number of records returned in a single call to poll()
      * @param maxPollInterval the maximum delay between invocations of poll() when using consumer group management
      * @param maxMetaDataAge period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions
      * @param autoOffsetReset what to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
      * @tparam K key deserialiser type
      * @tparam V value deserialiser type
      * @return consumer configuration consisting of all the given values
      */
    def apply[K, V](keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      bootstrapServers: String = "localhost:9092",
      groupId: String,
      enableAutoCommit: Boolean = true,
      autoCommitInterval: Int = 1000,
      sessionTimeoutMs: Int = 10000,
      maxPartitionFetchBytes: Int = ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES,
      maxPollRecords: Int = 500,
      maxPollInterval: Int = 300000,
      maxMetaDataAge : Long = 300000,             
      autoOffsetReset: OffsetResetStrategy = OffsetResetStrategy.LATEST,
      isolationLevel: IsolationLevel = IsolationLevel.READ_UNCOMMITTED
    ): Conf[K, V] = {

      val configMap = Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableAutoCommit.toString,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> autoCommitInterval.toString,
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> sessionTimeoutMs.toString,
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG -> maxPartitionFetchBytes.toString,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPollRecords.toString,
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> maxPollInterval.toString,
        ConsumerConfig.METADATA_MAX_AGE_CONFIG ->maxMetaDataAge.toString,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.toString.toLowerCase,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG -> isolationLevel.toString.toLowerCase()
      )

      apply(configMap, keyDeserializer, valueDeserializer)
    }

    /**
      * Creates a Kafka consumer configuration from a Typesafe config.
      *
      * The configuration names and values must match the Kafka's `ConsumerConfig` style.
      *
      * @param config a Typesafe config to build configuration from
      * @param keyDeserializer deserialiser for the key
      * @param valueDeserializer deserialiser for the value
      * @tparam K key deserialiser type
      * @tparam V value deserialiser type
      * @return consumer configuration
      */
    def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Conf[K, V] =
      apply(config.toPropertyMap, keyDeserializer, valueDeserializer)
  }

  /**
    * Configuration object for the Kafka consumer.
    *
    * The config is compatible with Kafka's `ConsumerConfig`.
    * All the key-value properties are specified in the given map, except the deserializers.
    * The key and value deserialiser instances are provided explicitly to ensure type-safety.
    *
    * @param props map of `ConsumerConfig` Properties
    * @tparam K key deserializer type
    * @tparam V value deserializer type
    */
  final case class Conf[K, V](
    props: Map[String, AnyRef],
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ) {

    /**
      * Extend the config with additional Typesafe config.
      * The supplied config overrides existing properties.
      */
    def withConf(config: Config): Conf[K, V] =
      copy(props = props ++ config.toPropertyMap)

    /**
      * Is auto commit mode in use.
      */
    def isAutoCommitMode: Boolean =
      props.getOrElse(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "").toString.equals("true")

    /**
      * Extend the configuration with a single key-value pair.
      */
    def withProperty(key: String, value: AnyRef): Conf[K, V] =
      copy(props = props + (key -> value))
  }

  /**
    * Create a Kafka consumer using the provided consumer configuration.
    *
    * @param conf configuration for the consumer
    * @tparam K key serialiser type
    * @tparam V value serialiser type
    * @return Kafka consumer client
    */
  def apply[K, V](conf: Conf[K, V]): JKafkaConsumer[K, V] = {
    conf.keyDeserializer.configure(conf.props.asJava, true)
    conf.valueDeserializer.configure(conf.props.asJava, false)
    new JKafkaConsumer[K, V](conf.props.asJava, conf.keyDeserializer, conf.valueDeserializer)
  }
}
