package cakesolutions.kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

object KafkaProducer {
  def apply[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) =
    new KafkaProducer[K, V](props, keySerializer, valueSerializer)

  def apply[K, V](bootstrapServers: String = "localhost:9092",
                  acks: String = "all",
                  retries: Int = 0,
                  batchSize: Int = 16384,
                  lingerMs: Int = 1,
                  bufferMemory: Int = 33554432,
                  keySerializer: Serializer[K] = new StringSerializer(),
                  valueSerializer: Serializer[V] = new StringSerializer()
                   ): KafkaProducer[K, V] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", acks)
    props.put("retries", retries.toString)
    props.put("batch.size", batchSize.toString)
    props.put("linger.ms", lingerMs.toString)
    props.put("buffer.memory", bufferMemory.toString)
    //    props.put("key.serializer", keySerializer)
    //    props.put("value.serializer", valueSerializer)

    new KafkaProducer(props, keySerializer, valueSerializer)
  }

  //TODO based on config
  def apply[K, V](config: Config) = ???

}

class KafkaProducer[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {

  import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer}

  val log: Logger = LoggerFactory.getLogger(getClass)

  val producer = new JKafkaProducer[K, V](props, keySerializer, valueSerializer)

  //TODO return future metadata
  def send(topic: String, key: K, value: V): Unit = {
    //TODO support scala Future[RecordMetadata]
    log.info("Sending message to topic=[{}], key=[{}], value=[{}]", topic, key.toString, value.toString)
    producer.send(kafkaMessage(topic, key, value))
    ()
  }

  //Send with callback
  def sendWithCallback(topic: String, key: K, value: V)(callback: RecordMetadata => Unit): Unit = {
    log.info("Sending message to topic: [{}], key=[{}], value=[{}]]", topic, key.toString, value.toString)
    producer.send(kafkaMessage(topic, key, value), producerCallback(callback))
    ()
  }

  def flush(): Unit = {
    producer.flush()
  }

  //TODO send with callback

  //TODO metrics?

  //TODO partitions for?

  private def kafkaMessage(topic: String, key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord(topic, key, value)
  }

  def close = {
    log.debug("Closing consumer")
    producer.close()
  }

  private def producerCallback(callback: RecordMetadata => Unit): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = callback(metadata)
    }
  }
}