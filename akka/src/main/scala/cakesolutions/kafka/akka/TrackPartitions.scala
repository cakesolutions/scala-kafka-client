package cakesolutions.kafka.akka

import java.util.{Collection => JCollection}

import akka.actor.ActorRef
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

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

  private var _offsets = Map[TopicPartition, Long]()
  private var _revoked = false

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsRevoked: " + partitions.toString)

    _revoked = true

    // If partitions have been revoked, keep a record of our current position with them.
    if (partitions.nonEmpty) {
      _offsets = partitions.map(partition => partition -> consumer.position(partition)).toMap
    }
  }

  def isRevoked = _revoked

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit = {
    log.debug("onPartitionsAssigned: " + partitions.toString)

    _revoked = false

    // If all of our previous partition assignments are present in the new assignment, we can continue uninterrupted by
    // seeking to the required offsets.  If we have lost any partition assignments (i.e to another group member), we
    // need to clear down the consumer actor state and proceed from the Kafka commit points.
    val allExisting = _offsets.forall { case (partition, _) => partitions.contains(partition) }

    if (!allExisting) {
      ref ! KafkaConsumerActor.RevokeReset
    } else {
      for {
        partition <- partitions
        offset <- _offsets.get(partition)
      } {
        log.info(s"Seeking partition: [{}] to offset [{}]",  partition, offset)
        consumer.seek(partition, offset)
      }
      ref ! KafkaConsumerActor.RevokeResume
    }
  }

  def reset(): Unit = {
    _offsets = Map.empty
    _revoked = false
  }
}