package cakesolutions.kafka

import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition

object KafkaTopicPartition {
  type Topic = String
  type Partition = Int

  /**
    * Create a Kafka topic-partition from given topic and partition pair.
    */
  def apply(topic: Topic, partition: Partition): TopicPartition = new TopicPartition(topic, partition)


  /**
    * Create a Kafka topic-partition from a given Typesafe config.
    */
  def fromConfig(config: Config): TopicPartition =
    KafkaTopicPartition(
      topic = config.getString("topic"),
      partition = config.getInt("partition")
    )
}
