package cakesolutions.kafka.akka

import java.util.{Collection => JCollection}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

private object TrackPartitions {
  private val log = LoggerFactory.getLogger(getClass)

  def apply(consumer: KafkaConsumer[_, _]): TrackPartitions = new TrackPartitions(consumer)
}

/**
 * Not thread safe. Can be used with Kafka consumer 0.9 API.
 */
private final class TrackPartitions(consumer: KafkaConsumer[_, _]) extends ConsumerRebalanceListener {
  import TrackPartitions.log

  private var _offsets = Map[TopicPartition, Long]()

  def seek(next: Map[TopicPartition, Long]): Unit = {
    log.info("Seeking to offsets: {}", next)
    _offsets = next
  }

  def clearOffsets(): Unit = _offsets.clear()

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit =
    _offsets = partitions.map(partition => partition -> consumer.position(partition)).toMap

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit =
    for {
      partition <- partitions
      offset <- _offsets.get(partition)
    } consumer.seek(partition, offset)
}
