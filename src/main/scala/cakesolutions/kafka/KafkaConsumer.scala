package cakesolutions.kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.slf4j.{Logger, LoggerFactory}

object KafkaConsumer {
  def apply[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): KafkaConsumer[K, V] =
    new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  //TODO how to enforce type params?
  def apply[K, V](bootstrapServers: String = "localhost:9092",
                  groupId: String = "test",
                  enableAutoCommit: Boolean = true,
                  autoCommitInterval: Int = 1000,
                  sessionTimeoutMs: Int = 30000,
                  keyDeserializer: Deserializer[K] = new StringDeserializer(),
                  valueDeserializer: Deserializer[V] = new StringDeserializer()
                   ): KafkaConsumer[K, V] = {

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", enableAutoCommit.toString)
    props.put("auto.commit.interval.ms", autoCommitInterval.toString)
    props.put("session.timeout.ms", sessionTimeoutMs.toString)
    //    props.put("key.deserializer", keyDeserializer)
    //    props.put("value.deserializer", valueDeserializer)

    new KafkaConsumer(props, keyDeserializer, valueDeserializer)
  }

  //TODO based on config
  def apply(config: Config) = ???
}

class KafkaConsumer[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {

  import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer}

  import scala.collection.JavaConversions._

  val log: Logger = LoggerFactory.getLogger(getClass)

  val consumer = new JKafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  //TODO list of topics
  def consume(topic: String)(write: (K, V) => Unit) = {
    log.info("Consuming")
    consumer.subscribe(List(topic))

    //TODO poll params
    val records: ConsumerRecords[K, V] = consumer.poll(100)
    log.info("Got [{}]", records.count())
    for (record <- records.iterator()) {
      log.info("Received msg from topic: [{}]", topic) //, key=[{}], value=[{}]", record.topic(), record.key().toString, record.value().toString)
      write(record.key(), record.value())
    }
  }

  def close = {
    log.debug("Closing consumer")
    consumer.close()
  }
}