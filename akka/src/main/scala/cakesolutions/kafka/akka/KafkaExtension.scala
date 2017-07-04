package cakesolutions.kafka.akka

import akka.actor.{ActorRef, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds

/**
  * Kafka extension provides starting points for building a consumer and producer
  */
trait KafkaExtension extends Extension {

  /**
    * Begins configuration of a consumer that will send the received messages from Kafka to the
    * given `downstreamActor`. If used inside an `Actor`, the implicit value is _this actor_.
    *
    * @param downstreamActor the actor that will receive the batches
    * @return the builder
    */
  def actorConsumer(implicit downstreamActor: ActorRef): ConsumerBuilder[ActorConsumer]

  /**
    * Begins configuration of a producer that will send the confirmations of sent batches to the
    * given `downstreamActor`. If used inside an `Actor`, the implicit value is _this actor_.
    *
    * @param downstreamActor the actor that will receive the send receipts
    * @return the builder
    */
  def actorProducer(implicit downstreamActor: ActorRef): ProducerBuilder[ActorProducer]

  /**
    * Begins configuration of a producer that will return futures of the sent batches.
    *
    * @return the builder
    */
  def futureProducer(): ProducerBuilder[FutureProducer]

}

/**
  * Builder that prepares an instance of `P` parametrized by the `K` and `V` types, indicating
  * the type of the keys and values to be produced
  */
trait ProducerBuilder[+P[_ >: Null, _]] {

  def build[K >: Null, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): P[K, V]

}

/**
  * Builder that prepares an instance of `C` parametrized by the `K` and `V` types, indicating
  * the type of the keys and values to be consumed
  */
trait ConsumerBuilder[+C[_, _]] {
  import scala.reflect.runtime.universe.TypeTag

  /**
    * Returns a builder with the `consumerGroup` property set
    * @param consumerGroup the consumer group to set
    * @return the updated builder
    */
  def withConsumerGroup(consumerGroup: String): ConsumerBuilder[C]

  /**
    * Verifies all properties and returns a built instance of `C[K, V]`
    *
    * @param keyDeserializer the key deserializer
    * @param valueDeserializer the value deserializer
    * @tparam K the type of keys
    * @tparam V the type of values
    * @return instance that can be used to subscribe to the messages from Kafka
    */
  def build[K: TypeTag, V: TypeTag](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): C[K, V]
}

/**
  * A consumer suitable for usage within an `Actor`. Typical usage is
  * {{{
  *   class MyActor extends Actor {
  *     private val extractor = KafkaExtension(context.system)
  *                               .actorConsumer
  *                               .build(extension StringDeserializer, extension StringDeserializer)
  *                               .autoPartition("test")
  *
  *     override def receive: Receive = {
  *       case extractor(records) ⇒
  *         // do some work with records
  *         sender() ! KafkaConsumerActor.Confirm(records.offsets)
  *     }
  *   }
  * }}}
  * @tparam K the key type
  * @tparam V the value type
  */
trait ActorConsumer[K, V] {

  /**
    * Subscribe to the given `topic` in auto-partitioning mode. When handling the messages
    * in the actor's `receive` function, do not forget to confirm the receipt of the batch
    * using `sender() ! KafkaConsumerActor.Confirm(records.offsets)`
    *
    * @param topics the topic names
    * @return the extractor that can be used in the `receive` method.
    */
  def autoPartition(topics: String*): Extractor[Any, ConsumerRecords[K, V]]

  /**
    * Subscribe to the given `topic` in auto-partitioning mode, but with manual offsets.
    * When handling the messages in the actor's `receive` function, do not forget to confirm the
    * receipt of the batch using `sender() ! KafkaConsumerActor.Confirm(records.offsets)`
    *
    * @param assignedListener the function to be called when Kafka assigns topic partitions to be consumed
    * @param revokedListener the function to be called when Kafka revokes assignment of topic partitions
    * @param topics the topic names
    * @return the extractor that can be used in the `receive` method.
    */
  def autoPartitionManualOffsets(assignedListener: List[TopicPartition] ⇒ Offsets,
    revokedListener: List[TopicPartition] ⇒ Unit,
    topics: String*): Extractor[Any, ConsumerRecords[K, V]]
}

/**
  * A producer suitable for usage within an `Actor`. Typical usage is
  * {{{
  *   class MyActor extends Actor {
  *     private val producer = KafkaExtension(system)
  *                              .actorProducer
  *                              .build(extension StringDeserializer, extension StringDeserializer)
  *
  *     override def receive: Receive = {
  *       case 'produce => producer.produce("topic", "key", "value", 'tag)
  *       case KafkaExtension.Produced(md, 'tag) => // produced successfully
  *       case KafkaExtension.NotProduced(ex, 'tag) => // not produced because of ex
  *     }
  *   }
  * }}}
  * @tparam K the key type
  * @tparam V the value type
  */
trait ActorProducer[K >: Null, V] {

  /**
    * Produce a single message on the given `topic` with the specified `key`, `value`; `tag` can
    * be used to correlate the delivery confirmations. If you do not need to correlate the responses
    * use `()`
    *
    * @param topic the topic
    * @param key the key
    * @param value the value
    * @param tag the tag
    */
  def produce(topic: String, key: K, value: V, tag: Any): Unit

  /**
    * Produce a a batch of messages; `tag` can be used to correlate the delivery confirmations.
    * If you do not need to correlate the responses use `()`
    *
    * @param records the records to be produced
    * @param tag the tag
    */
  def produce(records: Seq[ProducerRecord[K, V]], tag: Any): Unit
}

/**
  * A producer suitable for usage within a simple `ExecutionContext`
  *
  * @tparam K the key type
  * @tparam V the value type
  */
trait FutureProducer[K >: Null, V] {

  def produce(topic: String, key: K, value: V)(implicit executor: ExecutionContext): Future[RecordMetadata]

  def produce(topic: String, partition: Int, key: K, value: V)(implicit executor: ExecutionContext): Future[RecordMetadata]

  // def produce(records: Seq[ProducerRecord[K, V]])(implicit executor: ExecutionContext): Future[Seq[RecordMetadata]]

}

/**
  * Provides pooled & centrally supervised mechanism of dealing with Kafka.
  */
object KafkaExtension extends ExtensionId[KafkaExtension] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): KafkaExtension =
    new KafkaExtensionImpl(system)

  override def lookup(): ExtensionId[_ <: Extension] = KafkaExtension

  /**
    * Message delivered to an actor when a batch of records has been successfully produced.
    * @param metadata the metadata of the produced batch
    * @param tag the tag used in the produce request
    * @see Producer#produce
    */
  case class Produced(metadata: Seq[RecordMetadata], tag: Any)

  /**
    * Message delivered to an actor when a batch of records has failed to be sent.
    * @param ex the reason for the failure
    * @param tag the tag used in the produce request
    */
  case class NotProduced(ex: Throwable, tag: Any)

}