package cakesolutions.kafka

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata, KafkaProducer => JKafkaProducer}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConversions._

object KafkaProducer {

  object Conf {
    def apply[K, V](keySerializer: Serializer[K],
                    valueSerializer: Serializer[V],
                    bootstrapServers: String = "localhost:9092",
                    acks: String = "all",
                    retries: Int = 0,
                    batchSize: Int = 16384,
                    lingerMs: Int = 1,
                    bufferMemory: Int = 33554432): Conf[K, V] = {

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

    def apply[K, V](config: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Conf[K, V] =
      Conf(config.toPropertyMap, keySerializer, valueSerializer)
  }

  /**
    * Configuration object for the KafkaProducer. Key and Value serialisers are provided explicitly.
    *
    * @param props Map of KafkaProducer Properties. Usually created via the Object helpers.
    * @tparam K Key Serializer type
    * @tparam V Value Serializer type
    */
  case class Conf[K, V](props: Map[String, AnyRef],
                        keySerializer: Serializer[K],
                        valueSerializer: Serializer[V]) {

    /**
      * With additional config defined in supplied Typesafe config.  Supplied config overrides existing properties
      *
      * @param config Typesafe Config
      */
    def withConf(config: Config): Conf[K, V] = {
      copy(props = props ++ config.toPropertyMap)
    }

    /**
      * Returns Conf with additional property.
      */
    def withProperty(key: String, value: AnyRef) = {
      copy(props = props + (key -> value))
    }
  }

  def apply[K, V](conf: Conf[K, V]): KafkaProducer[K, V] = {
    apply(new JKafkaProducer[K, V](conf.props, conf.keySerializer, conf.valueSerializer))
  }

  def apply[K, V](producer: JKafkaProducer[K, V]): KafkaProducer[K, V] =
    new KafkaProducer(producer)
}

/**
  * Wraps the [[JKafkaProducer]] providing send operations that indicate the result of the operation with either a
  * Scala [[Future]] or a Function callback.
  *
  * @param producer The underlying [[JKafkaProducer]]
  * @tparam K Key Serializer type
  * @tparam V Value Deserializer type
  */
class KafkaProducer[K, V](val producer: JKafkaProducer[K, V]) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Asynchronously send a record to a topic, providing a [[Future]] to contain the result of the operation.
    *
    * @param record ProducerRecord to sent
    * @return
    */
  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    producer.send(record, producerCallback(promise))
    promise.future
  }

  /**
    * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
    *
    * @param record   ProducerRecord to sent
    * @param callback Callback when the send has been acknowledged.
    */
  def sendWithCallback(record: ProducerRecord[K, V])(callback: Try[RecordMetadata] => Unit): Unit = {
    producer.send(record, producerCallback(callback))
  }

  def flush(): Unit = {
    producer.flush()
  }

  def partitionsFor(topic: String): List[PartitionInfo] = {
    producer.partitionsFor(topic).toList
  }

  def close() = {
    log.debug("Closing consumer")
    producer.close()
  }

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