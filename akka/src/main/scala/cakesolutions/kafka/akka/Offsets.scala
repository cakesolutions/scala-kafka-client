package cakesolutions.kafka.akka

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

object Offsets {
  def empty: Offsets = Offsets(Map.empty)
}

/**
  * Map of partitions to partition offsets.
  */
case class Offsets(offsetsMap: Map[TopicPartition, Long]) extends AnyVal {

  /**
    * Get offset for a topic & partition pair.
    *
    * @param topic topic + partition
    * @return offset or `None`
    */
  def get(topic: TopicPartition): Option[Long] = offsetsMap.get(topic)

  /**
    * Convert offsets to map of Kafka commit offsets
    */
  def toCommitMap: Map[TopicPartition, OffsetAndMetadata] =
    offsetsMap.mapValues(offset => new OffsetAndMetadata(offset))

  override def toString: String =
    offsetsMap
      .map { case (t, o) => s"$t: $o" }
      .mkString("Offsets(", ", ", ")")
}

trait HasOffsets {
  /**
    * The offsets assigned to the client after the records were pulled from Kafka.
    */
  val offsets: Offsets
}

