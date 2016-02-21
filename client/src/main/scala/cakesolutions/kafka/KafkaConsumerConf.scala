package cakesolutions.kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

object KafkaConsumerConf {
//  def apply[K, V](props: Properties): KafkaConsumerConf[K, V] =
//    new KafkaConsumerConf[K, V](props)
//
//  def apply[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): KafkaConsumerConf[K, V] =
//    new KafkaConsumerConf[K, V](props, keyDeserializer, valueDeserializer)

//  def apply[K, V](keyDeserializer: Deserializer[K],
//                  valueDeserializer: Deserializer[V],
//                  bootstrapServers: String = "localhost:9092",
//                  groupId: String = "test",
//                  enableAutoCommit: Boolean = true,
//                  autoCommitInterval: Int = 1000,
//                  sessionTimeoutMs: Int = 30000): KafkaConsumerConf[K, V] = {
//
//    val props = new Properties()
//    props.put("bootstrap.servers", bootstrapServers)
//    props.put("group.id", groupId)
//    props.put("enable.auto.commit", enableAutoCommit.toString)
//    props.put("auto.commit.interval.ms", autoCommitInterval.toString)
//    props.put("session.timeout.ms", sessionTimeoutMs.toString)
//    apply(props, keyDeserializer, valueDeserializer)
//  }

//  def apply[K, V](config: Config): KafkaConsumerConf[K, V] =
//    apply(config.toProperties)
}

case class KafkaConsumerConf[K, V](keyDeserializer: Deserializer[K],
                                   valueDeserializer: Deserializer[V],
                                   bootstrapServers: String = "localhost:9092",
                                   groupId: String,
                                   enableAutoCommit: Boolean = true,
                                   autoCommitInterval: Int = 1000,
                                   sessionTimeoutMs: Int = 30000) {

}