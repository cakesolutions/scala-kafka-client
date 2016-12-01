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

  object AutoPartition {
    /**
      * Subscribe to topics in auto-assigned partition mode.
      *
      * @see [[AutoPartition]]
      */
    def apply(topic1: String, topics: String*): AutoPartition = AutoPartition(topic1 +: topics)
  }

  /**
    * Subscribe to topics in auto assigned partition mode, relying on Kafka to manage the commit point for each partition.
    * This is this simplest and most common subscription mode that provides a parallel streaming capability with at-least-once
    * semantics.
    *
    * In auto assigned partition mode, the consumer partitions are managed by Kafka.
    * This means that they can get automatically rebalanced with other consumers consuming from the same topic with the same group-id.
    *
    * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
    *
    * The client should ensure that received records are confirmed with 'commit = true' to ensure kafka tracks the commit point.
    *
    * @param topics the topics to subscribe to start consuming from
    */
  final case class AutoPartition(topics: Iterable[String]) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = this
  }

  object AutoPartitionWithManualOffset {

  }

  /**
    * Subscribe to topics in auto assigned partition mode with client managed offset commit positions for each partition.
    * This subscription mode is typically used when performing some parallel stateful computation and storing the offset
    * position along with the state in some kind of persistent store.  This allows for exactly-once state manipulation against
    * an at-least-once delivery stream.
    *
    * The client should provide callbacks to receive notifications of when partitions have been assigned or revoked.  When
    * a partition has been assigned, the client should lookup the latest offsets for the given partitions from its store, and supply
    * those.  The KafkaConsumerActor will seek to the specified positions.
    *
    * The client should ensure that received records are confirmed with 'commit = false' to ensure consumed records are
    * not committed back to kafka.
    *
    * @param topics the topics to subscribe to start consuming from
    * @param assignedListener a callback handler that should lookup the latest offsets for the provided topic/partitions.
    * @param revokedListener a callback to provide the oppurtunity to cleanup any in memory state for revoked partitions.
    */
  final case class AutoPartitionWithManualOffset(topics: Iterable[String],
    assignedListener: List[TopicPartition] => Offsets,
    revokedListener: List[TopicPartition] => Unit) extends Subscribe {

    override def advance(offsets: Offsets): Subscribe = this
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
    * Subscribe to topics in manually assigned partition mode, relying on Kafka to manage the commit point for each partition.
    *
    * In manually assigned partition mode, the consumer will specify the partitions directly, this means that Kafka will not be automatically
    * rebalance the partitions when new consumers appear in the consumer group.
    *
    * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
    *
    * The client should ensure that received records are confirmed with 'commit = true' to ensure kafka tracks the commit point.
    *
    * @param topicPartitions the topics with partitions to start consuming from
    */
  final case class ManualPartition(topicPartitions: Iterable[TopicPartition]) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = ManualOffset(offsets)
  }

  /**
    * Subscribe to topics in manually assigned partition mode, with client managed offset commit positions for each partition.
    *
    * In manually assigned partition mode, the consumer will specify the partitions directly,
    * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
    *
    * In addition to manually assigning the partitions, the partition offsets will be set to start from the given offsets.
    *
    * The client should ensure that received records are confirmed with 'commit = false' to ensure consumed records are
    * not committed back to kafka.
    *
    * @param offsets the topics with partitions and offsets to start consuming from
    */
  final case class ManualOffset(offsets: Offsets) extends Subscribe {
    override def advance(offsets: Offsets): Subscribe = ManualOffset(offsets)
  }
}