package cakesolutions.kafka.akka

import scala.reflect.runtime.universe.{TypeTag, typeTag}
import cakesolutions.kafka.KafkaProducerRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord => JConsumerRecord, ConsumerRecords => JConsumerRecords}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

/**
  * Helper functions for [[ConsumerRecords]].
  */
object ConsumerRecords {
  type Pair[Key, Value] = (Option[Key], Value)
  type Partition = (String, Int)

  /**
    * Create consumer records for a single partition from values only.
    *
    * This constructor function is not used by the library.
    * It's useful for generating data for testing purposes in cases where you don't want to involve the [[KafkaConsumerActor]].
    *
    * The offsets will contain only one partition.
    * The partition offset will be set according to the size of the given sequence.
    */
  def fromValues[Key >: Null : TypeTag, Value: TypeTag](partition: Partition, values: Seq[Value]): ConsumerRecords[Key, Value] =
    fromPairs(partition, values.map(None -> _))

  /**
    * Create consumer records for a single partition from key-value pairs.
    *
    * This constructor function is not used by the library.
    * It's useful for generating data for testing purposes in cases where you don't want to involve the [[KafkaConsumerActor]].
    *
    * The offsets will contain only one partition.
    * The partition offset will be set according to size of the given sequence.
    */
  def fromPairs[Key >: Null : TypeTag, Value: TypeTag](partition: Partition, pairs: Seq[Pair[Key, Value]]): ConsumerRecords[Key, Value] =
    fromMap(Map(partition -> pairs))

  /**
    * Create consumer records from a map of partitions and key-value pairs.
    *
    * This constructor function is not used by the library.
    * It's useful for generating data for testing purposes in cases where you don't want to involve the [[KafkaConsumerActor]].
    *
    * The partition offsets will be set according to the number of messages in a partition.
    */
  def fromMap[Key >: Null : TypeTag, Value: TypeTag](values: Map[Partition, Seq[Pair[Key, Value]]]): ConsumerRecords[Key, Value] = {
    def createConsumerRecords(topic: String, partition: Int, pairs: Seq[Pair[Key, Value]]) =
      pairs.zipWithIndex.map {
        case ((key, value), offset) =>
          new JConsumerRecord[Key, Value](topic, partition, offset, key.orNull, value)
      }

    val recordsMap = values.map {
      case ((topic, partition), pairs) =>
        val tp = new TopicPartition(topic, partition)
        val rs = createConsumerRecords(topic, partition, pairs)
        tp -> rs
    }

    val offsets = Offsets(recordsMap.mapValues(_.maxBy(_.offset()).offset()))

    val records = new JConsumerRecords(recordsMap.mapValues(_.asJava).asJava)

    ConsumerRecords(offsets, records)
  }

  /**
    * Create an extractor for pattern matching any value with a specific [[ConsumerRecords]] type.
    *
    * Example:
    * {{{
    * val partition = ("sometopic", 0)
    * val ext = ConsumerRecords.extractor[String, Int]
    * val kvs1: Any = ConsumerRecords.fromPairs(partition, Seq(Some("foo") -> 1))  // KeyValues[String, Int]
    * val kvs2: Any = ConsumerRecords.fromPairs(partition, Seq(Some(9) -> "asdf")) // KeyValues[Int, String]
    *
    * kvs1 match {
    *   case ext(kvs) => println(kvs == kvs1) // `kvs` has type KeyValues[String, Int] here
    *   case _ =>
    * }
    * // prints "true"
    *
    * kvs2 match {
    *   case ext(kvs) => println(kvs == kvs2) // no match here
    *   case _ => print("match fail")
    * }
    * // prints "match fail"
    * }}}
    *
    * @tparam Key the type of the key to match in the extractor
    * @tparam Value the type of the value to match in the extractor
    * @return an extractor for given key and value types
    */
  def extractor[Key: TypeTag, Value: TypeTag]: Extractor[Any, ConsumerRecords[Key, Value]] =
    Extractor {
      case rs: ConsumerRecords[_, _] => rs.cast[Key, Value]
      case _ => None
    }
}

/**
  * Records consumed from Kafka by [[KafkaProducerActor]].
  *
  * @param offsets the last offsets of all the subscribed partitions after consuming the records from Kafka
  * @param records the records consumed from Kafka
  * @tparam Key type of the key in records
  * @tparam Value type of the value in records
  */
case class ConsumerRecords[Key: TypeTag, Value: TypeTag](offsets: Offsets, records: JConsumerRecords[Key, Value])
  extends HasOffsets {

  import ConsumerRecords.Pair

  val keyTag: TypeTag[Key] = typeTag[Key]
  val valueTag: TypeTag[Value] = typeTag[Value]

  /**
    * Compare given types to key-value types.
    *
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherKey the key type to compare to
    * @tparam OtherValue the value type to compare to
    * @return true when given types match object's type parameters, and false otherwise
    */
  def hasType[OtherKey: TypeTag, OtherValue: TypeTag]: Boolean =
    typeTag[OtherKey].tpe <:< keyTag.tpe &&
      typeTag[OtherValue].tpe <:< valueTag.tpe

  /**
    * Attempt to cast key-value pairs to given types.
    *
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherKey the key type to cast to
    * @tparam OtherValue the value type to cast to
    * @return the same records in casted form when casting is possible, and otherwise `None`
    */
  def cast[OtherKey: TypeTag, OtherValue: TypeTag]: Option[ConsumerRecords[OtherKey, OtherValue]] =
    if (hasType[OtherKey, OtherValue]) Some(this.asInstanceOf[ConsumerRecords[OtherKey, OtherValue]])
    else None

  /**
    * Convert to Kafka's `ProducerRecord`s.
    *
    * @param topic the Kafka topic to use for the records
    * @return a sequence of Kafka's `ProducerRecord`s
    */
  def toProducerRecords(topic: String): Seq[ProducerRecord[Key, Value]] =
    pairs.map { case (k, v) => KafkaProducerRecord(topic, k, v) }

  /**
    * Convert only values to Kafka's `ProducerRecord`.
    *
    * @param topic the Kafka topic to use for the records
    * @return a sequence of Kafka's `ProducerRecord`s
    */
  def valuesToProducerRecords(topic: String): Seq[ProducerRecord[Nothing, Value]] =
    values.map { v => KafkaProducerRecord(topic, v) }

  /**
    * All the records as a list.
    */
  val recordsList: List[JConsumerRecord[Key, Value]] = records.asScala.toList

  /**
    * All the keys and values as a sequence.
    */
  val pairs: Seq[Pair[Key, Value]] = recordsList.map(r => (Option(r.key()), r.value()))

  /**
    * All the values as a sequence.
    */
  val values: Seq[Value] = recordsList.map(_.value())

  /**
    * The number of records.
    */
  val size: Int = records.count()
}
