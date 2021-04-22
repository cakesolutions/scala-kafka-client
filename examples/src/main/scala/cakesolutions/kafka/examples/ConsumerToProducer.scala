package com.pirum.kafka.examples

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.pirum.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.pirum.kafka.akka._
import com.pirum.kafka.{KafkaConsumer, KafkaProducer}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.{
  StringDeserializer,
  StringSerializer
}

import scala.concurrent.duration._

/** This example demonstrates an At-Least-Once delivery pipeline from Kafka topic: 'topic1' to 'topic2'.
  * KafkaConsumerActor is used with an AutoPartition subscription, records are send via a KafkaProducerActor
  * and committed back to source when confirmed.
  *
  * If multiple instances of this application are started, Kafka will balance the topic and data across the applications (parallel streaming).
  *
  * Kafka bootstrap server can be provided as an environment variable: -DKAFKA=127.0.0.1:9092 (default).
  */
object ConsumerToProducerBoot extends App {
  val config = ConfigFactory.load()
  ConsumerToProducer(config.getConfig("consumer"), config.getConfig("producer"))
}

object ConsumerToProducer {

  /*
   * Starts an ActorSystem and instantiates the below Actor that subscribes and
   * consumes from the configured KafkaConsumerActor and pipelines records directly to another topic via
   * the configured KafkaProducerActor.
   */
  def apply(consumerConfig: Config, producerConfig: Config): ActorRef = {

    // Create KafkaConsumerActor config with bootstrap.servers specified in Typesafe config
    val consumerConf = KafkaConsumer
      .Conf(
        new StringDeserializer,
        new StringDeserializer,
        groupId = "test_group",
        enableAutoCommit = false,
        autoOffsetReset = OffsetResetStrategy.EARLIEST
      )
      .withConf(consumerConfig)

    val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds, 5)

    // Create KafkaProducerActor config with defaults and bootstrap.servers specified in Typesafe config
    val producerConf = KafkaProducer
      .Conf(new StringSerializer, new StringSerializer)
      .withConf(producerConfig)

    val system = ActorSystem()
    system.actorOf(
      Props(new ConsumerToProducer(consumerConf, actorConf, producerConf))
    )
  }
}

class ConsumerToProducer(
    kafkaConfig: KafkaConsumer.Conf[String, String],
    actorConfig: KafkaConsumerActor.Conf,
    producerConf: KafkaProducer.Conf[String, String]
) extends Actor
    with ActorLogging {

  private val recordsExt = ConsumerRecords.extractor[String, String]

  // The KafkaConsumerActor
  private val consumer = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )
  context.watch(consumer)

  // The KafkaProducerActor
  private val producer = context.actorOf(KafkaProducerActor.props(producerConf))

  consumer ! Subscribe.AutoPartition(List("topic1"))

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      processRecords(records)

    // Confirmed Offsets from KafkaProducer
    case o: Offsets =>
      consumer ! Confirm(o, commit = true)
  }

  // Demonstrates some transformation of the messages before forwarding to KafkaProducer
  private def processRecords(records: ConsumerRecords[String, String]) = {
    val transformedRecords = records.pairs.map { case (key, value) =>
      (key, value + ".")
    }

    // Send records to Topic2.  Offsets will be sent back to this actor once confirmed.
    producer ! ProducerRecords.fromKeyValues[String, String](
      "topic2",
      transformedRecords,
      Some(records.offsets),
      None
    )

    // Could have sent them like this if we didn't first transform:
    // producer ! ProducerRecords.fromConsumerRecords("topic2", records, None)
  }
}
