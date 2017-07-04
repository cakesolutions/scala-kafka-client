package cakesolutions.kafka.akka

import akka.actor.{Actor, ActorLogging, ActorRef, ExtendedActorSystem, Extension, Props, Terminated}
import akka.event.EventStream
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Random, Success, Try}

/** INTERNAL APIs and implementations */

private[kafka] class KafkaExtensionImpl(system: ExtendedActorSystem) extends Extension with KafkaExtension {

  import KafkaExtensionImpl._

  private val consumerSupervisor: ActorRef = system.actorOf(KafkaConsumerSupervisor.props)
  //  private val producerSupervisor: ActorRef = system.actorOf(KafkaProducerSupervisor.props)

  def actorConsumer(implicit downstreamActor: ActorRef): ConsumerBuilder[ActorConsumer] = {
    val config = Try(system.settings.config.getConfig("app.kafka.consumer-config")).getOrElse(ConfigFactory.empty())
    ActorConsumerBuilder(config, downstreamActor, consumerSupervisor, Map.empty)
  }

  def actorConsumer(implicit downstreamActor: ActorRef, configPath: String): ConsumerBuilder[ActorConsumer] = {
    val config = Try(system.settings.config.getConfig(configPath)).getOrElse(ConfigFactory.empty())
    ActorConsumerBuilder(config, downstreamActor, consumerSupervisor, Map.empty)
  }

  def actorProducer(implicit downstreamActor: ActorRef): ProducerBuilder[ActorProducer] = {
    val config = Try(system.settings.config.getConfig("app.kafka.producer-config")).getOrElse(ConfigFactory.empty())
    ActorProducerBuilder(config, downstreamActor, system.eventStream)(system.dispatcher)
  }

  override def futureProducer(): ProducerBuilder[FutureProducer] = {
    val config = Try(system.settings.config.getConfig("app.kafka.producer-config")).getOrElse(ConfigFactory.empty())
    FutureProducerBuilder(config, system.eventStream)
  }
}

private object KafkaExtensionImpl {

  object KafkaProducers {
    private var producers: Map[String, KafkaProducer[_, _]] = Map.empty

    def getOrConfigureProducer[K, V](conf: KafkaProducer.Conf[K, V]): KafkaProducer[K, V] = {
      synchronized {
        val key = conf.props.toString()
        producers.get(key) match {
          case Some(producer) ⇒
            producer.asInstanceOf[KafkaProducer[K, V]]
          case None ⇒
            val producer = KafkaProducer(conf)
            producers += (key → producer)
            producer
        }
      }
    }

  }

  case class ActorProducerBuilder private(config: Config, downstreamActor: ActorRef, eventStream: EventStream)(implicit executor: ExecutionContext)
    extends ProducerBuilder[ActorProducer] {

    override def build[K >: Null, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): ActorProducerImpl[K, V] = {
      val producerConf = KafkaProducer.Conf(config, keySerializer, valueSerializer)
      ActorProducerImpl[K, V](KafkaProducers.getOrConfigureProducer(producerConf), downstreamActor, eventStream)
    }

  }

  // Why event bus?
  case class FutureProducerBuilder private(config: Config, eventStream: EventStream)
    extends ProducerBuilder[FutureProducer] {

    override def build[K >: Null, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): FutureProducerImpl[K, V] = {
      val producerConf = KafkaProducer.Conf(config, keySerializer, valueSerializer)
      FutureProducerImpl[K, V](KafkaProducers.getOrConfigureProducer(producerConf), eventStream)
    }

  }

  case class ActorProducerImpl[K >: Null, V] private(kafkaProducer: KafkaProducer[K, V], downstreamActor: ActorRef, eventStream: EventStream)(implicit executor: ExecutionContext) extends ActorProducer[K, V] {

    override def produce(topic: String, key: K, value: V, tag: Any): Unit = {
      val record = KafkaProducerRecord(topic, key, value)
      kafkaProducer.send(record).onComplete {
        case Success(rmd) ⇒
          eventStream.publish(Seq(record))
          if (downstreamActor != null) downstreamActor ! KafkaExtension.Produced(Seq(rmd), tag)
        case Failure(ex) ⇒
          if (downstreamActor != null) downstreamActor ! KafkaExtension.NotProduced(ex, tag)
      }
    }

    override def produce(records: Seq[ProducerRecord[K, V]], tag: Any): Unit = {
      Future.sequence(records.map(kafkaProducer.send)).onComplete {
        case Success(rmd) ⇒
          eventStream.publish(records)
          if (downstreamActor != null) downstreamActor ! KafkaExtension.Produced(rmd, tag)
        case Failure(ex) ⇒
          if (downstreamActor != null) downstreamActor ! KafkaExtension.NotProduced(ex, tag)
      }
    }

  }

  case class FutureProducerImpl[K >: Null, V] private(kafkaProducer: KafkaProducer[K, V], eventStream: EventStream) extends FutureProducer[K, V] {

    override def produce(topic: String, partition: Int, key: K, value: V)(implicit executor: ExecutionContext): Future[RecordMetadata] = {
      val tps = KafkaProducerRecord.Destination(topic, partition)
      val record = KafkaProducerRecord[K, V](tps, Some(key), value)
      eventStream.publish(Seq(record))
      kafkaProducer.send(record)
    }

    override def produce(topic: String, key: K, value: V)(implicit executor: ExecutionContext): Future[RecordMetadata] = {
      val record = KafkaProducerRecord[K, V](topic, key, value)
      eventStream.publish(Seq(record))
      kafkaProducer.send(record)
    }

  }

  object KafkaConsumerSupervisor {
    val props: Props = Props[KafkaConsumerSupervisor]

    case class AddConsumer(downstreamActor: ActorRef, subscribe: KafkaConsumerActor.Subscribe, consumerProps: Props)

    private case class ResubscribeConsumerNow(consumerActor: ActorRef, subscribe: KafkaConsumerActor.Subscribe)
    private case class ResubscribeConsumer(consumerActor: ActorRef, subscribe: KafkaConsumerActor.Subscribe)
  }

  class KafkaConsumerSupervisor extends Actor with ActorLogging {

    import KafkaConsumerSupervisor._

    private var consumers: Map[ActorRef, ActorRef] = Map.empty

    private def resubscribingSubscription(consumerActor: ActorRef, subscribe: KafkaConsumerActor.Subscribe): KafkaConsumerActor.Subscribe = {
      def resubscribingAssignedListener(underlyingAL: List[TopicPartition] ⇒ Offsets)(tps: List[TopicPartition]): Offsets = {
        if (tps.isEmpty) {
          log.warning("No topicPartitions... scheduling re-subscription.")
          self ! ResubscribeConsumer(consumerActor, subscribe)
        }

        underlyingAL(tps)
      }

      subscribe match {
        case KafkaConsumerActor.Subscribe.AutoPartitionWithManualOffset(topics, al, rl) ⇒
          KafkaConsumerActor.Subscribe.AutoPartitionWithManualOffset(topics, resubscribingAssignedListener(al), rl)
        case x ⇒ x
      }
    }

    override def receive: Receive = {
      case AddConsumer(downstreamActor, subscribe, consumerProps) ⇒
        consumers.get(downstreamActor) match {
          case Some(kca) ⇒
            // we already have subscription
            kca ! subscribe
          case None ⇒
            // we're subscribing for the first time
            val kca = context.actorOf(consumerProps)
            consumers = consumers + (kca → downstreamActor)
            kca ! resubscribingSubscription(kca, subscribe)
            context.watch(downstreamActor)
        }

      case ResubscribeConsumer(consumerActor, subscribe) ⇒
        consumerActor ! KafkaConsumerActor.Unsubscribe

        import scala.concurrent.duration._
        val in = (5 + Random.nextDouble()).seconds
        log.info("Consumer {} has no subscriptions. Attempting to re-subscribe in {}", consumerActor, in)

        import context.dispatcher
        context.system.scheduler.scheduleOnce(in, self, ResubscribeConsumerNow(consumerActor, subscribe))

      case ResubscribeConsumerNow(consumerActor, subscribe) ⇒
        log.info("Resubscribing {}", consumerActor)
        consumerActor ! resubscribingSubscription(consumerActor, subscribe)

      case Terminated(downstreamActor) ⇒
        log.info(s"Terminating subscriptions for $downstreamActor")
        consumers.get(downstreamActor) match {
          case None ⇒ // there's not much to do
          case Some(ksa) ⇒ context.stop(ksa)
        }
    }

  }

  case class ActorConsumerBuilder private(config: Config, downstreamActor: ActorRef, supervisorActor: ActorRef, properties: Map[String, AnyRef])
    extends ConsumerBuilder[ActorConsumer] {

    override def withConsumerGroup(consumerGroup: String): ActorConsumerBuilder = copy(properties = properties + (ConsumerConfig.GROUP_ID_CONFIG → consumerGroup))

    override def build[K: TypeTag, V: TypeTag](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): ActorConsumerImpl[K, V] = {
      val consumerConf = properties.foldLeft(KafkaConsumer.Conf(config, keyDeserializer, valueDeserializer)) {
        case (conf, (k, v)) ⇒ conf.withProperty(k, v)
      }
      val consumerActorConf = KafkaConsumerActor.Conf()

      ActorConsumerImpl(supervisorActor, downstreamActor, consumerConf, consumerActorConf)
    }

  }

  case class ActorConsumerImpl[K: TypeTag, V: TypeTag] private(supervisorActor: ActorRef, downstreamActor: ActorRef, consumerConf: KafkaConsumer.Conf[K, V], consumerActorConf: KafkaConsumerActor.Conf)
    extends ActorConsumer[K, V] {

    override def autoPartitionManualOffsets(assignedListener: List[TopicPartition] ⇒ Offsets, revokedListener: List[TopicPartition] ⇒ Unit, topics: String*): Extractor[Any, ConsumerRecords[K, V]] = {
      val props = KafkaConsumerActor.props(consumerConf = consumerConf, actorConf = consumerActorConf, downstreamActor = downstreamActor)
      val subscribe = KafkaConsumerActor.Subscribe.AutoPartitionWithManualOffset(topics.toIterable, assignedListener, revokedListener)
      supervisorActor ! KafkaConsumerSupervisor.AddConsumer(downstreamActor, subscribe, props)
      ConsumerRecords.extractor[K, V]
    }

    override def autoPartition(topics: String*): Extractor[Any, ConsumerRecords[K, V]] = {
      val props = KafkaConsumerActor.props(consumerConf = consumerConf, actorConf = consumerActorConf, downstreamActor = downstreamActor)
      val subscribe = KafkaConsumerActor.Subscribe.AutoPartition(topics.toSeq)
      supervisorActor ! KafkaConsumerSupervisor.AddConsumer(downstreamActor, subscribe, props)
      ConsumerRecords.extractor[K, V]
    }

  }

}