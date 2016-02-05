package net.cakesolutions.kafka.akka

import java.util.{Collection => JCollection}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.mutable

object TrackPartitions {
  def apply(consumer: KafkaConsumer[_, _]): TrackPartitions = new TrackPartitions(consumer)
}

/**
  * Not thread safe. Can be used with Kafka consumer 0.9 API.
  */
class TrackPartitions(consumer: KafkaConsumer[_, _]) extends ConsumerRebalanceListener {

  private val offsets = mutable.Map[TopicPartition, Long]()

  def getOffsets = offsets

  def clearOffsets(): Unit = offsets.clear()

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit =
    partitions.foreach { partition =>
      offsets += (partition -> consumer.position(partition))
    }

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit =
    for {
      partition <- partitions
      offset <- offsets.get(partition)
    } consumer.seek(partition, offset)
}
