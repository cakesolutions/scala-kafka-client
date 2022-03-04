package com.pirum.kafka.akka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/** Helper functions for [[Offsets]].
  */
object Offsets {

  /** Offsets map with no offset information.
    */
  def empty: Offsets = Offsets(Map.empty)
}

/** Map of partitions to partition offsets.
  *
  * [[KafkaConsumerActor]] sends [[Offsets]] along with a batch of data related to the offsets.
  * The offsets in this case represent the point the Kafka consumer reached after consuming the batch.
  * Because the offsets are unique to the consumed batch,
  * the offsets are also used as a confirmation key for confirming that the batch has been fully processed.
  */
final case class Offsets(offsetsMap: Map[TopicPartition, Long]) extends AnyVal {

  /** Get offset for a topic & partition pair.
    *
    * @param topic topic + partition
    * @return offset or `None`
    */
  def get(topic: TopicPartition): Option[Long] = offsetsMap.get(topic)

  /** Convert offsets to map of Kafka commit offsets
    */
  def toCommitMap: Map[TopicPartition, OffsetAndMetadata] =
    offsetsMap.view.mapValues(offset => new OffsetAndMetadata(offset)).toMap

  /** Keep only the offsets for partitions that are also in the given set of partitions.
    *
    * @param tps partitions to filter the offsets with
    * @return filtered offsets
    */
  def keepOnly(tps: Set[TopicPartition]): Offsets =
    copy(offsetsMap.filter { case (t, _) => tps(t) })

  /** Remove the given partitions from the offsets.
    *
    * @param tps partitions to remove
    * @return filtered offsets
    */
  def remove(tps: Set[TopicPartition]): Offsets =
    copy(offsetsMap -- tps)

  /** Whether there are no offsets.
    */
  def isEmpty: Boolean = offsetsMap.isEmpty

  /** Whether there are any offsets.
    */
  def nonEmpty: Boolean = offsetsMap.nonEmpty

  /** Set of partitions in the offsets.
    */
  def topicPartitions: Set[TopicPartition] = offsetsMap.keySet

  override def toString: String =
    offsetsMap
      .map { case (t, o) => s"$t = $o" }
      .mkString("Offsets(", ", ", ")")
}

/** An object that contains [[Offsets]].
  */
trait HasOffsets {

  /** The offsets assigned to the client after the records were pulled from Kafka.
    */
  val offsets: Offsets
}
