package cakesolutions.kafka.examples.extension

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import cakesolutions.kafka.akka.KafkaExtension
import org.apache.kafka.common.serialization.StringDeserializer

object AutoPartitionConsumerBoot extends App {
  ActorSystem().actorOf(ConsumerToProducer.props, "MyConsumer")
}

object AutoPartitionConsumer {
  def props: Props = Props(new ConsumerToProducer)
}

class AutoPartitionConsumer extends Actor with ActorLogging {

  val extractor = KafkaExtension(context.system)
    .actorConsumer.build(new StringDeserializer(), new StringDeserializer())
    .autoPartition("topic1")

  override def receive: Receive = {
    case extractor(records) =>
      processRecords(records.pairs)
      sender() ! Confirm(records.offsets, commit = true)
  }

  private def processRecords(records: Seq[(Option[String], String)]) =
    records.foreach { case (key, value) =>
      log.info(s"Received [$key,$value]")
    }
}


