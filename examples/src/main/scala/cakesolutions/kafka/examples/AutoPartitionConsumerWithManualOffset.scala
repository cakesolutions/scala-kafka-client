package com.pirum.examples

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.pirum.KafkaConsumer
import com.pirum.akka.KafkaConsumerActor._
import com.pirum.akka.{ConsumerRecords, KafkaConsumerActor, Offsets}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

/** Simple Kafka Consumer using AutoPartition subscription mode with manual offset control, subscribing to topic: 'topic1'.
  *
  * If the topic is configured in Kafka with multiple partitions, this app can be started multiple times (potentially on separate nodes)
  * and Kafka will balance the partitions to the instances providing parallel consumption of the topic.
  *
  * Kafka bootstrap server can be provided as an environment variable: -DKAFKA=127.0.0.1:9092 (default).
  */
object AutoPartitionConsumerWithManualOffsetBoot extends App {
  AutoPartitionConsumerWithManualOffset(
    ConfigFactory.load().getConfig("consumer")
  )
}

object AutoPartitionConsumerWithManualOffset {

  /*
   * Starts an ActorSystem and instantiates the below Actor that subscribes and
   * consumes from the configured KafkaConsumerActor.
   */
  def apply(config: Config): ActorRef = {
    val consumerConf = KafkaConsumer
      .Conf(
        new StringDeserializer,
        new StringDeserializer,
        groupId = "test_group",
        enableAutoCommit = false,
        autoOffsetReset = OffsetResetStrategy.EARLIEST
      )
      .withConf(config)

    val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds)

    val system = ActorSystem()
    system.actorOf(
      Props(new AutoPartitionConsumerWithManualOffset(consumerConf, actorConf))
    )
  }
}

class AutoPartitionConsumerWithManualOffset(
    kafkaConfig: KafkaConsumer.Conf[String, String],
    actorConfig: KafkaConsumerActor.Conf
) extends Actor
    with ActorLogging {

  private val recordsExt = ConsumerRecords.extractor[String, String]

  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )

  consumer ! Subscribe.AutoPartitionWithManualOffset(
    List("topic1"),
    assignedListener,
    revokedListener
  )

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      processRecords(records.pairs)
      sender() ! Confirm(records.offsets)
  }

  private def processRecords(records: Seq[(Option[String], String)]) =
    records.foreach { case (key, value) =>
      log.info(s"Received [$key,$value]")
    }

  private def assignedListener(tps: List[TopicPartition]): Offsets = {
    log.info("Partitions have been assigned" + tps.toString())

    // Should load the offsets from a persistent store and any related state
    val offsetMap = tps.map { tp =>
      tp -> 0L
    }.toMap

    // Return the required offsets for the assigned partitions
    Offsets(offsetMap)
  }

  private def revokedListener(tps: List[TopicPartition]): Unit = {
    log.info("Partitions have been revoked" + tps.toString())
    // Opportunity to clear any state for the revoked partitions
    ()
  }
}
