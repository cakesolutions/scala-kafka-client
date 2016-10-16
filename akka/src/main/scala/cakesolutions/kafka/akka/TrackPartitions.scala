package cakesolutions.kafka.akka

import java.util.{Collection => JCollection}

import akka.actor.ActorRef
import cakesolutions.kafka.{KafkaConsumer, Offsets}
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private object TrackPartitions {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(consumer: KafkaConsumer[_, _], ref: ActorRef): TrackPartitions = new TrackPartitions(consumer, ref)
}

/**
  * Listens to partition change events coming from Kafka driver.  A best-effort is made to continue processing once
  * reassignment is complete without causing duplications.  Due to limitations in the driver it is not possible in all
  * cases to allow buffered messages to flush through prior to the partition reassignment completing.
  *
  * @param consumer The client driver
  * @param ref Tha KafkaConsumerActor to notify of partition change events
  */
private final class TrackPartitions(consumer: KafkaConsumer[_, _], ref: ActorRef) extends ConsumerRebalanceListener {

  import TrackPartitions.log

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

  def isRevoked = _revoked

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsAssigned: " + partitions.toString)

    _revoked = false

    // If all of our previous partition assignments are present in the new assignment, we can continue uninterrupted by
    // seeking to the required offsets.  If we have lost any partition assignments (i.e to another group member), we
    // need to clear down the consumer actor state and proceed from the Kafka commit points.
    val allExisting = _offsets.topicPartitions.forall { partition => partitions.contains(partition) }

    if (!allExisting) {
      ref ! KafkaConsumerActor.RevokeReset
    } else {
      consumer.seekOffsets(_offsets.keepOnly(partitions.asScala.toSet))
      ref ! KafkaConsumerActor.RevokeResume
    }
  }

  def reset(): Unit = {
    _offsets = Offsets.empty
    _revoked = false
  }
}