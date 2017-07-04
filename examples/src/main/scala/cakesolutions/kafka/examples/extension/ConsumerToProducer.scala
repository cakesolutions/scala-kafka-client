package cakesolutions.kafka.examples.extension

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import cakesolutions.kafka.akka.KafkaConsumerActor.Confirm
import cakesolutions.kafka.akka.KafkaExtension
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object ConsumerToProducerBoot extends App {
  ActorSystem().actorOf(ConsumerToProducer.props, "MyConsumer")
}

object ConsumerToProducer {
  def props: Props = Props(new ConsumerToProducer)
}

class ConsumerToProducer extends Actor with ActorLogging {

  val extractor = KafkaExtension(context.system)
    .actorConsumer.build(new StringDeserializer(), new StringDeserializer())
    .autoPartition("topic1")

  val producer = KafkaExtension(context.system)
    .actorProducer
    .build(new StringSerializer(), new StringSerializer())

  override def receive: Receive = {
    case extractor(records) =>
      processRecords(records.pairs)



      sender() ! Confirm(records.offsets, commit = true)
  }

  private def processRecords(records: Seq[(Option[String], String)]) =
    records.foreach { case (key, value) =>
      log.info(s"Received [$key,$value]")
        producer.produce("topic2", key.get, value, ())
    }
}

// new default conf
// KafkaExtension(context.system).actorConsumer
//  .withDeserializers(new StringDeserializer(), new StringDeserializer())
//  .build()

// specified config path
// KafkaExtension(context.system).actorConsumer
//  .withConfigPath("consumer")
//  .withDeserializers(new StringDeserializer(), new StringDeserializer())
//  .build()

// case class configuration
// KafkaExtension(context.system).actorConsumer
//  .withConfig(config, actorConfig)
//  .build()

