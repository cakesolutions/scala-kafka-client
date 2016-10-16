package cakesolutions.kafka

import org.apache.kafka.common.TopicPartition

sealed trait Subscribe {

  /**
    * Advance subscription based on last confirmed offsets.
    */
  def advance(offsets: Offsets): Subscribe
}

/**
  * Kafka consumer subscription modes.
  *
  *  - Auto partition: provide the topics to subscribe to, and let Kafka manage partition delegation between consumers.
  *  - Manual partition: provide the topics with partitions, and let Kafka decide the point where to begin consuming messages from.
  *  - Manual offset: provide the topics with partitions and offsets for precise control.
  */
object Subscribe {

  /**
    * Subscribe to topics in auto-assigned partition mode.
    *
    * In auto assigned partition mode, the consumer partitions are managed by Kafka.
    * This means that they can get automatically rebalanced with other consumers consuming from the same topic.
    *
    * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
    *
    * @param topics the topics to subscribe to start consuming from
    */
  final case class AutoPartition(topics: Iterable[String]) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = this
  }

  object AutoPartition {
    /**
      * Subscribe to topics in auto-assigned partition mode.
      *
      * @see [[AutoPartition]]
      */
    def apply(topic1: String, topics: String*): AutoPartition = AutoPartition(topic1 +: topics)
  }

  /**
    * Subscribe to topics in manually assigned partition mode.
    *
    * In manually assigned partition mode, the consumer partitions are managed by the client.
    * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
    *
    * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
    *
    * @param topicPartitions the topics with partitions to start consuming from
    */
  final case class ManualPartition(topicPartitions: Iterable[TopicPartition]) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = ManualOffset(offsets)
  }

  object ManualPartition {
    /**
      * Subscribe to topics in manually assigned partition mode.
      *
      * @see [[ManualPartition]]
      */
    def apply(topicPartition1: TopicPartition, topicPartitions: TopicPartition*): ManualPartition =
      ManualPartition(topicPartition1 +: topicPartitions)

    /**
      * Subscribe to topics in manually assigned partition mode.
      *
      * Partition #0 will be subscribed to in all the given topics.
      *
      * @see [[ManualPartition]]
      * @param topics list of topics to subscribe to
      */
    def withTopics(topics: Iterable[String]): ManualPartition =
      ManualPartition(topics.map(t => KafkaTopicPartition(t, 0)))

    /**
      * Subscribe to topics in manually assigned partition mode.
      *
      * Partition #0 will be subscribed to in all the given topics.
      *
      * @see [[ManualPartition]]
      */
    def withTopics(topic1: String, topics: String*): ManualPartition =
      withTopics(topic1 +: topics)
  }

  /**
    * Subscribe to topics in manually assigned partition mode, and set the offsets to consume from.
    *
    * In manually assigned partition mode, the consumer partitions are managed by the client.
    * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
    *
    * In addition to manually assigning the partitions, the partition offsets will be set to start from the given offsets.
    *
    * @param offsets the topics with partitions and offsets to start consuming from
    */
  final case class ManualOffset(offsets: Offsets) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = ManualOffset(offsets)
  }
}