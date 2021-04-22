package com.pirum.kafka.examples

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.pirum.kafka.KafkaConsumer
import com.pirum.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.pirum.kafka.akka.{ConsumerRecords, KafkaConsumerActor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

/** Simple Kafka Consumer using AutoPartition subscription mode, subscribing to topic: 'topic1'.
  *
  * If the topic is configured in Kafka with multiple partitions, this app can be started multiple times (potentially on separate nodes)
  * and Kafka will balance the partitions to the instances providing parallel consumption of the topic.
  *
  * Kafka bootstrap server can be provided as an environment variable: -DKAFKA=127.0.0.1:9092 (default).
  */
object AutoPartitionConsumerBoot extends App {
  AutoPartitionConsumer(ConfigFactory.load().getConfig("consumer"))
}

object AutoPartitionConsumer {

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
    system.actorOf(Props(new AutoPartitionConsumer(consumerConf, actorConf)))
  }
}

class AutoPartitionConsumer(
    kafkaConfig: KafkaConsumer.Conf[String, String],
    actorConfig: KafkaConsumerActor.Conf
) extends Actor
    with ActorLogging {

  private val recordsExt = ConsumerRecords.extractor[String, String]

  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )
  context.watch(consumer)

  consumer ! Subscribe.AutoPartition(List("topic1"))

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      processRecords(records.pairs)
      sender() ! Confirm(records.offsets, commit = true)
  }

  private def processRecords(records: Seq[(Option[String], String)]) =
    records.foreach { case (key, value) =>
      log.info(s"Received [$key,$value]")
    }
}
