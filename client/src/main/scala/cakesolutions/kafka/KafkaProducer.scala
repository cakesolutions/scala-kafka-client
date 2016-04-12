package cakesolutions.kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer}
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.{Logger, LoggerFactory}
import cakesolutions.kafka.TypesafeConfigExtensions._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

object KafkaProducer {
  def apply[K, V](producer: JKafkaProducer[K, V]): KafkaProducer[K, V] =
    new KafkaProducer(producer)

  def apply[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]): KafkaProducer[K, V] =
    apply(new JKafkaProducer[K, V](props, keySerializer, valueSerializer))

  def apply[K, V](props: Properties): KafkaProducer[K, V] =
    apply(new JKafkaProducer[K, V](props))

  def apply[K, V](keySerializer: Serializer[K],
                  valueSerializer: Serializer[V],
                  bootstrapServers: String = "localhost:9092",
                  acks: String = "all",
                  retries: Int = 0,
                  batchSize: Int = 16384,
                  lingerMs: Int = 1,
                  bufferMemory: Int = 33554432): KafkaProducer[K, V] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", acks)
    props.put("retries", retries.toString)
    props.put("batch.size", batchSize.toString)
    props.put("linger.ms", lingerMs.toString)
    props.put("buffer.memory", bufferMemory.toString)
    apply(props, keySerializer, valueSerializer)
  }

  def apply[K, V](config: Config): KafkaProducer[K, V] =
    apply(config.toProperties)
}

class KafkaProducer[K, V](val producer: JKafkaProducer[K, V]) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    producer.send(record, producerCallback(promise))
    promise.future
  }

  def sendWithCallback(record: ProducerRecord[K, V])(callback: Try[RecordMetadata] => Unit): Unit = {
    producer.send(record, producerCallback(callback))
  }

  def flush(): Unit = {
    producer.flush()
  }

  //TODO metrics?

  //TODO partitions for?

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