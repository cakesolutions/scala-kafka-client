package net.cakesolutions.kafka.akka

import akka.actor.{ActorRef, Props}
import scala.reflect.runtime.universe._

object KafkaActor {

  def consumer[K: TypeTag, V: TypeTag](conf: KafkaConsumerActor.Conf[K, V], nextActor: ActorRef): Props = {
    Props(new KafkaConsumerActor[K, V](conf, nextActor))
  }
}
