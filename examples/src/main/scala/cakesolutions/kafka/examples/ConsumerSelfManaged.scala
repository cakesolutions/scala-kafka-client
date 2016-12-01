package cakesolutions.kafka.examples

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import cakesolutions.kafka.{KafkaConsumer, Offsets, Subscribe}
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import cakesolutions.kafka.akka.{ConsumerRecords, Extractor, KafkaConsumerActor}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

/**
  * Simple Kafka Consumer using ManualPartition subscription mode, subscribing to topic: 'topic1'.
  *
  * Kafka bootstrap server can be provided as an environment variable: -DKAFKA=127.0.0.1:9092 (default).
  */
object ConsumerSelfManagedBood extends App {
  ConsumerSelfManaged
}

object ConsumerSelfManaged {

  /*
   * Starts an ActorSystem and instantiates the below Actor that subscribes and
   * consumes from the configured KafkaConsumerActor.
   */
  def apply(config: Config): ActorRef = {
    val consumerConf = KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      groupId = "groupId",
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST)
      .withConf(config)

    val actorConf = KafkaConsumerActor.Conf(1.seconds, 3.seconds)

    val system = ActorSystem()
    system.actorOf(Props(new ConsumerSelfManaged(consumerConf, actorConf)))
  }
}

class ConsumerSelfManaged(
  kafkaConfig: KafkaConsumer.Conf[String, String],
  actorConfig: KafkaConsumerActor.Conf) extends Actor with ActorLogging {

  val recordsExt: Extractor[Any, ConsumerRecords[String, String]] = ConsumerRecords.extractor[String, String]

  val consumer: ActorRef = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )

  consumer ! Subscribe.ManualOffset(Offsets(Map((new TopicPartition("topic1", 0), 1))))

  override def receive: Receive = {

    // Records from Kafka
    case recordsExt(records) =>
      processRecords(records)
      sender() ! Confirm(records.offsets)
  }

  private def processRecords(records: ConsumerRecords[String, String]) = {
    records.pairs.foreach { case (key, value) =>
      log.info(s"Received [$key,$value]")
    }
    log.info(s"Batch complete, offsets: ${records.offsets}")
  }
}
