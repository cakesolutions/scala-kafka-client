package com.pirum.examples

import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  ActorSystem,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import com.pirum.KafkaConsumer
import com.pirum.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.pirum.akka.{ConsumerRecords, Extractor, KafkaConsumerActor}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

/** A simple consumer example with a configured Supervision Strategy to handle failures of the Kafka driver.
  */
object ConsumerRecoveryBoot extends App {
  ConsumerRecovery(ConfigFactory.load().getConfig("consumer"))
}

object ConsumerRecovery {
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
    system.actorOf(Props(new ConsumerRecovery(consumerConf, actorConf)))
  }
}

class ConsumerRecovery(
    kafkaConfig: KafkaConsumer.Conf[String, String],
    actorConfig: KafkaConsumerActor.Conf
) extends Actor
    with ActorLogging {

  /** Provide a Supervision Strategy for handling failures in the KafkaConsumerActor.  This simple strategy is configured
    * to retry fatal exceptions up to 10 times, and then escalate the issues if still unable to progress, which will result
    * in the application terminating.
    */
  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10) {
      case _: KafkaConsumerActor.ConsumerException =>
        log.info("Consumer exception caught. Restarting consumer.")
        SupervisorStrategy.Restart
      case _ =>
        SupervisorStrategy.Escalate
    }

  val recordsExt: Extractor[Any, ConsumerRecords[String, String]] =
    ConsumerRecords.extractor[String, String]

  val consumer: ActorRef = context.actorOf(
    KafkaConsumerActor.props(kafkaConfig, actorConfig, self)
  )

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
