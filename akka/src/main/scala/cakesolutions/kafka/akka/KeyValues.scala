package cakesolutions.kafka.akka

import cakesolutions.kafka.KafkaProducerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe.{TypeTag, typeTag}

object KeyValues {
  type Pair[Key, Value] = (Option[Key], Value)

  /**
    * Create key-value pairs where all the values share the same key.
    *
    * @param key the key that the values share
    * @param values sequence of values
    */
  def apply[Key: TypeTag, Value: TypeTag](key: Option[Key], values: Seq[Value]): KeyValues[Key, Value] =
    apply(values.map(key -> _))

  /**
    * Create key-value pairs from a sequence of pairs.
    */
  def apply[Key: TypeTag, Value: TypeTag](keyValues: Seq[Pair[Key, Value]]): KeyValues[Key, Value] =
    PlainKeyValues(keyValues)

  /**
    * Create key-value pairs from Kafka client's `ConsumerRecords`.
    */
  def apply[Key: TypeTag, Value: TypeTag](records: ConsumerRecords[Key, Value]): KeyValues[Key, Value] =
    KafkaConsumerKeyValues(records)

  /**
    * Create an extractor for pattern matching any value with a specific [[KeyValues]] type.
    *
    * Example:
    * {{{
    * val ext = KeyValues.extractor[String, Int]
    * val kvs1: Any = KeyValues(Some("foo"), Seq(1, 2, 3))  // KeyValues[String, Int]
    * val kvs2: Any = KeyValues(Some(9), Seq("asdf"))       // KeyValues[Int, String]
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
  def extractor[Key: TypeTag, Value: TypeTag]: Extractor[Any, KeyValues[Key, Value]] =
    Extractor {
      case kvs: KeyValues[_, _] => kvs.cast[Key, Value]
      case _ => None
    }

  /**
    * A collection of key-value pairs created from any sequence.
    *
    * Used for producing [[KafkaProducerActor]] compatible records in the user code.
    *
    * @param pairs a sequence of key-value pairs
    */
  case class PlainKeyValues[Key: TypeTag, Value: TypeTag](pairs: Seq[KeyValues.Pair[Key, Value]])
    extends KeyValues[Key, Value]

  /**
    * A collection of key-value pairs produced by [[KafkaConsumerActor]].
    *
    * @param records `ConsumerRecords` produced by [[KafkaConsumerActor]]
    */
  case class KafkaConsumerKeyValues[Key: TypeTag, Value: TypeTag](records: ConsumerRecords[Key, Value])
    extends KeyValues[Key, Value] {

    /**
      * All the records from [[records]] in a list.
      */
    lazy val recordsList = records.toList

    override def pairs: Seq[Pair] = recordsList.map(r => (Some(r.key()), r.value()))

    override def values: Seq[Value] = recordsList.map(_.value())

    override def size: Int = records.count()
  }
}

/**
  * A collection of key-value pairs with type tags for runtime type comparison.
  *
  * Produced by [[KafkaConsumerActor]] and consumed by [[KafkaProducerActor]].
  */
sealed abstract class KeyValues[Key: TypeTag, Value: TypeTag] {
  type Pair = KeyValues.Pair[Key, Value]

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
  def cast[OtherKey: TypeTag, OtherValue: TypeTag]: Option[KeyValues[OtherKey, OtherValue]] =
    if (hasType[OtherKey, OtherValue]) Some(this.asInstanceOf[KeyValues[OtherKey, OtherValue]])
    else None

  /**
    * All the values without the keys.
    */
  def values: Seq[Value] = pairs.map(_._2)

  /**
    * Convert key-value pairs to Kafka `ProducerRecord`.
    *
    * @param topic the Kafka topic to use for the records
    * @return a sequence of Kafka `ProducerRecord`s
    */
  def toProducerRecords(topic: String): Seq[ProducerRecord[Key, Value]] =
    pairs.map { case (k, v) => KafkaProducerRecord(topic, k, v) }

  /**
    * Convert values from key-value pairs to Kafka `ProducerRecord`.
    *
    * @param topic the Kafka topic to use for the records
    * @return a sequence of Kafka `ProducerRecord`s
    */
  def valuesToProducerRecords(topic: String): Seq[ProducerRecord[Nothing, Value]] =
    pairs.map { case (_, v) => KafkaProducerRecord(topic, v) }

  /**
    * Number of key-value pairs
    */
  def size: Int = pairs.size

  /**
    * All the keys-value pairs in a sequence.
    */
  def pairs: Seq[Pair]
}

object KeyValuesWithOffsets {
  import KeyValues.Pair

  /**
    * Create key-value pairs where all the values share the same key.
    *
    * @param offsets the offsets associated with the values and the key
    * @param key the key that the values share
    * @param values sequence of values
    */
  def apply[Key: TypeTag, Value: TypeTag](offsets: Offsets, key: Option[Key], values: Seq[Value]): KeyValuesWithOffsets[Key, Value] =
    apply(offsets, KeyValues(key, values))

  /**
    * Create key-value pairs from a sequence of pairs along with given offsets.
    */
  def apply[Key: TypeTag, Value: TypeTag](offsets: Offsets, keyValues: Seq[Pair[Key, Value]]): KeyValuesWithOffsets[Key, Value] =
    apply(offsets, KeyValues(keyValues))

  /**
    * Create key-value pairs from `ConsumerRecords` along with given offsets.
    */
  def apply[Key: TypeTag, Value: TypeTag](offsets: Offsets, records: ConsumerRecords[Key, Value]): KeyValuesWithOffsets[Key, Value] =
    apply(offsets, KeyValues(records))

  /**
    * Create an extractor for pattern matching any value with a specific [[KeyValuesWithOffsets]] type.
    *
    * {{{
    * val ext = KeyValuesWithOffsets.extractor[String, Int]
    * val offsets = Offsets.empty
    * val kvs1: Any = KeyValuesWithOffsets(offsets, "foo", Seq(1, 2, 3))  // KeyValuesWithOffsets[String, Int]
    * val kvs2: Any = KeyValuesWithOffsets(offsets, 9, Seq("asdf"))       // KeyValuesWithOffsets[Int, String]
    *
    * kvs1 match {
    *   case ext(kvs) => // `kvs` has type KeyValuesWithOffsets[String, Int] here
    *     println(kvs.offsets == offsets && kvs.keyValues == kvs1.keyValues)
    *   case _ => println("match fail")
    * }
    * // prints "true"
    *
    * kvs2 match {
    *   case ext(kvs) => // no match here
    *     println(kvs.offsets == offsets && kvs.keyValues == kvs2.keyValues)
    *   case _ => print("match fail")
    * }
    * // prints "match fail"
    * }}}
    *
    * @tparam Key the type of the key to match in the extractor
    * @tparam Value the type of the value to match in the extractor
    * @return an extractor for given key and value types
    */
  def extractor[Key: TypeTag, Value: TypeTag]: Extractor[Any, KeyValuesWithOffsets[Key, Value]] =
    Extractor {
      case rs: KeyValuesWithOffsets[_, _] =>
        rs.keyValues.cast[Key, Value]
          .map(kvs => KeyValuesWithOffsets(rs.offsets, kvs))
      case _ => None
    }
}

/**
  * A collection of key-value pairs with type tags for runtime type comparison and offsets from Kafka consumer.
  *
  * Produced by [[KafkaConsumerActor]].
  * [[KafkaConsumerActor]] fills the [[offsets]] field with the offsets from the consumer driver.
  */
case class KeyValuesWithOffsets[Key: TypeTag, Value: TypeTag](offsets: Offsets, keyValues: KeyValues[Key, Value])
  extends HasOffsets
