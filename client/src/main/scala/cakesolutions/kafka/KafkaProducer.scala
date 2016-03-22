package cakesolutions.kafka

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata, KafkaProducer => JKafkaProducer}
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
      apply(config.toPropertyMap, keySerializer, valueSerializer)
  }

  /**
    * Configuration object for the KafkaProducer. Key and Value serialisers are provided explicitly.
    * @param props Map of KafkaProducer Properties. Usually created via the Object helpers.
    * @tparam K Key Serializer type
    * @tparam V Value Serializer type
    */
  case class Conf[K, V](props: Map[String, AnyRef],
                        keySerializer: Serializer[K],
                        valueSerializer: Serializer[V]) {

    def withConf(config: Config): Conf[K, V] = {
      copy(props = props ++ config.toPropertyMap)
    }

    def withProperty(key: String, value: AnyRef) = {
      copy(props = props + (key -> value))
    }
  }

  def apply[K, V](conf: Conf[K, V]): KafkaProducer[K, V] = {
    KafkaProducer(new JKafkaProducer[K, V](conf.props, conf.keySerializer, conf.valueSerializer))
  }

  def apply[K, V](producer: JKafkaProducer[K, V]): KafkaProducer[K, V] =
    new KafkaProducer(producer)
}

class KafkaProducer[K, V](val producer: JKafkaProducer[K, V]) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    logSend(record)
    producer.send(record, producerCallback(promise))
    promise.future
  }

  def sendWithCallback(record: ProducerRecord[K, V])(callback: Try[RecordMetadata] => Unit): Unit = {
    logSend(record)
    producer.send(record, producerCallback(callback))
  }

  private def logSend(record: ProducerRecord[K, V]): Unit = {
    val key = Option(record.key()).map(_.toString).getOrElse("null")
    log.debug("Sending message to topic=[{}], key=[{}], value=[{}]",
      record.topic(), key, record.value().toString)
  }

  def flush(): Unit = {
    producer.flush()
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