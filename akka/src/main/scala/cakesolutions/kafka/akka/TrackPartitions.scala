package cakesolutions.kafka.akka

import java.util.{Collection => JCollection}

import akka.actor.ActorRef
import cakesolutions.kafka.{KafkaConsumer, Offsets}
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

sealed trait TrackPartitions extends ConsumerRebalanceListener {
  def isRevoked: Boolean

  def reset(): Unit
}

/**
  * Listens to partition change events coming from Kafka driver.  A best-effort is made to continue processing once
  * reassignment is complete without causing duplications.  Due to limitations in the driver it is not possible in all
  * cases to allow buffered messages to flush through prior to the partition reassignment completing.
  *
  * This class is used when using commit mode, i.e. relying on Kafka to manage commit points.
  *
  * @param consumer      The client driver
  * @param consumerActor Tha KafkaConsumerActor to notify of partition change events
  */
private final class TrackPartitionsCommitMode(
  consumer: KafkaConsumer[_, _],
  consumerActor: ActorRef ) extends TrackPartitions {

  private val log = LoggerFactory.getLogger(getClass)

  private var _offsets = Offsets.empty
  private var _revoked = false

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsRevoked: " + partitions.toString)

    _revoked = true

    // If partitions have been revoked, keep a record of our current position with them.
    if (!partitions.isEmpty) {
      _offsets = consumer.offsets(partitions.asScala)
    }
  }

  def isRevoked: Boolean = _revoked

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsAssigned: " + partitions.toString)

    _revoked = false

    // If all of our previous partition assignments are present in the new assignment, we can continue uninterrupted by
    // seeking to the required offsets.  If we have lost any partition assignments (i.e to another group member), we
    // need to clear down the consumer actor state and proceed from the Kafka commit points.
    val allExisting = _offsets.topicPartitions.forall { partition => partitions.contains(partition) }

    if (!allExisting) {
      consumerActor ! KafkaConsumerActor.RevokeReset
    } else {
      consumer.seekOffsets(_offsets.keepOnly(partitions.asScala.toSet))
      consumerActor ! KafkaConsumerActor.RevokeResume
    }
  }

  def reset(): Unit = {
    _offsets = Offsets.empty
    _revoked = false
  }
}

/**
  * Listens to partition change events coming from Kafka driver.  A best-effort is made to continue processing once
  * reassignment is complete without causing duplications.  Due to limitations in the driver it is not possible in all
  * cases to allow buffered messages to flush through prior to the partition reassignment completing.
  *
  * This class is used when using a manual offset subscription types, i.e. the commit points are managed by the client.
  *
  * @param consumer         The client driver
  * @param consumerActor    Tha KafkaConsumerActor to notify of partition change events
  * @param assignedListener Callback to the client when new partitions have been assigned to this consumer
  * @param revokedListener  Callback to the client when partitions have been revoked from the consumer
  */
private final class TrackPartitionsManualOffset(
  consumer: KafkaConsumer[_, _], consumerActor: ActorRef,
  assignedListener: List[TopicPartition] => Offsets,
  revokedListener: List[TopicPartition] => Unit) extends TrackPartitions {

  private val log = LoggerFactory.getLogger(getClass)

  private var _offsets = Offsets.empty
  private var _revoked = false

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsRevoked: " + partitions.toString)

    _revoked = true

    if (!partitions.isEmpty) {
      _offsets = consumer.offsets(partitions.asScala)
    }
  }

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsAssigned: " + partitions.toString)

    _revoked = false

    // If all of our previous partition assignments are present in the new assignment, we can continue uninterrupted by
    // seeking to the required offsets.  If we have lost any partition assignments (i.e to another group member), we
    // need to clear down the consumer actor state.
    val allExisting = _offsets.topicPartitions.forall { partition => partitions.contains(partition) }

    if (allExisting) {
      val newPartitions = partitions.asScala.toList.diff(_offsets.topicPartitions.toList)
      val offsets = assignedListener(newPartitions)
      consumer.seekOffsets(offsets)
    } else {
      consumerActor ! KafkaConsumerActor.RevokeReset

      // Invoke client callback to notify revocation of all existing partitions.
      revokedListener(_offsets.topicPartitions.toList)

      // Invoke client callback to notify the new assignments and seek to the provided offsets.
      _offsets = assignedListener(partitions.asScala.toList)
      consumer.seekOffsets(_offsets)
    }
  }

  override def isRevoked: Boolean = _revoked

  def reset(): Unit = {
    _revoked = false
  }
}

private final class EmptyTrackPartitions extends TrackPartitions {
  override def isRevoked: Boolean = false

  def reset(): Unit = {}

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = throw new IllegalStateException("TrackPartitions not initialised")

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit = throw new IllegalStateException("TrackPartitions not initialised")
}