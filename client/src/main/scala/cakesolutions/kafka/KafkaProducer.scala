package cakesolutions.kafka

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata, KafkaProducer => JKafkaProducer, Producer => JProducer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Utilities for creating a Kafka producer.
  *
  * This companion object provides tools for creating Kafka producers.
  */
object KafkaProducer {

  /**
    * Utilities for creating Kafka producer configurations.
    */
  object Conf {

    /**
      * Kafka producer configuration constructor with common configurations as parameters.
      * For more detailed configuration, use the other [[Conf]] constructors.
      *
      * @param keySerializer the serialiser for the key
      * @param valueSerializer the serialiser for the value
      * @param bootstrapServers a list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
      * @param acks the number of acknowledgments the producer requires the leader to have received before considering a request complete
      * @param retries how many times sending is retried
      * @param batchSize the size of the batch of sent messages in bytes
      * @param lingerMs how long will the producer wait for additional messages before it sends a batch
      * @param bufferMemory the total bytes of memory the producer can use to buffer records waiting to be sent to the server
      * @tparam K key serialiser type
      * @tparam V value serialiser type
      * @return producer configuration consisting of all the given values
      */
    def apply[K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      bootstrapServers: String = "localhost:9092",
      acks: String = "all",
      retries: Int = 0,
      batchSize: Int = 16384,
      lingerMs: Int = 1,
      bufferMemory: Int = 33554432
    ): Conf[K, V] = {

      val configMap = Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ProducerConfig.ACKS_CONFIG -> acks,
        ProducerConfig.RETRIES_CONFIG -> retries.toString,
        ProducerConfig.BATCH_SIZE_CONFIG -> batchSize.toString,
        ProducerConfig.LINGER_MS_CONFIG -> lingerMs.toString,
        ProducerConfig.BUFFER_MEMORY_CONFIG -> bufferMemory.toString
      )

      apply(configMap, keySerializer, valueSerializer)
    }

    /**
      * Creates a Kafka producer configuration from a Typesafe config.
      *
      * The configuration names and values must match the Kafka's `ProducerConfig` style.
      *
      * @param config a Typesafe config to build configuration from
      * @param keySerializer serialiser for the key
      * @param valueSerializer serialiser for the value
      * @tparam K key serialiser type
      * @tparam V value serialiser type
      * @return consumer configuration
      */
    def apply[K, V](config: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Conf[K, V] =
      Conf(config.toPropertyMap, keySerializer, valueSerializer)
  }

  /**
    * Configuration object for the Kafka producer.
    *
    * The config is compatible with Kafka's `ProducerConfig`.
    * All the key-value properties are specified in the given map, except the serializers.
    * The key and value serialiser instances are provided explicitly to ensure type-safety.
    *
    * @param props map of `ProducerConfig` properties
    * @tparam K key serializer type
    * @tparam V value serializer type
    */
  final case class Conf[K, V](
    props: Map[String, AnyRef],
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ) {

    /**
      * Extend the config with additional Typesafe config.
      * The supplied config overrides existing properties.
      */
    def withConf(config: Config): Conf[K, V] = {
      copy(props = props ++ config.toPropertyMap)
    }

    /**
      * Extend the configuration with a single key-value pair.
      */
    def withProperty(key: String, value: AnyRef): Conf[K, V] = {
      copy(props = props + (key -> value))
    }
  }

  /**
    * Create [[KafkaProducer]] from given configurations.
    *
    * @param conf the configurations for the producer
    * @tparam K type of the key that the producer accepts
    * @tparam V type of the value that the producer accepts
    * @return Kafka producer instance
    */
  def apply[K, V](conf: Conf[K, V]): KafkaProducer[K, V] =
    apply(new JKafkaProducer[K, V](conf.props.asJava, conf.keySerializer, conf.valueSerializer))

  /**
    * Create [[KafkaProducer]] from a given Java `KafkaProducer` object.
    *
    * @param producer Java `KafkaProducer` object
    * @tparam K type of the key that the producer accepts
    * @tparam V type of the value that the producer accepts
    * @return Kafka producer instance
    */
  def apply[K, V](producer: JProducer[K, V]): KafkaProducer[K, V] =
    new KafkaProducer(producer)
}

/**
  * Wraps the Java `KafkaProducer`
  * providing send operations that indicate the result of the operation with either
  * a Scala `Future` or a Function callback.
  *
  * @param producer the underlying Java `KafkaProducer`
  * @tparam K type of the key that the producer accepts
  * @tparam V type of the value that the producer accepts
  */
final class KafkaProducer[K, V](val producer: JProducer[K, V]) {

  /**
    * Asynchronously send a record to a topic, providing a `Future` to contain the result of the operation.
    *
    * @param record `ProducerRecord` to sent
    * @return the results of the sent records as a `Future`
    */
  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    try {
      producer.send(record, producerCallback(promise))
    } catch {
      case NonFatal(e) => promise.failure(e)
    }

    promise.future
  }

  /**
    * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
    *
    * @param record `ProducerRecord` to sent
    * @param callback callback that is called when the send has been acknowledged
    */
  def sendWithCallback(record: ProducerRecord[K, V])(callback: Try[RecordMetadata] => Unit): Unit = {
    producer.send(record, producerCallback(callback))
  }

  /**
    * Make all buffered records immediately available to send and wait until records have been sent.
    *
    * @see Java `KafkaProducer` [[http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#flush() flush]] method
    */
  def flush(): Unit =
    producer.flush()

  /**
    * Get the partition metadata for the give topic.
    *
    * @see Java `KafkaProducer` [[http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#partitionsFor(java.lang.String) partitionsFor]] method
    */
  def partitionsFor(topic: String): List[PartitionInfo] =
    producer.partitionsFor(topic).asScala.toList

  /**
    * Close this producer.
    *
    * @see Java `KafkaProducer` [[http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close() close]] method
    */
  def close(): Unit =
    producer.close()

  private def producerCallback(promise: Promise[RecordMetadata]): Callback =
    producerCallback(result => promise.complete(result))

  private def producerCallback(callback: Try[RecordMetadata] => Unit): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result =
          if (exception == null) Success(metadata)
          else Failure(exception)
        callback(result)
      }
    }
  }
}