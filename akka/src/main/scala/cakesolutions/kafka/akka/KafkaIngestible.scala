package cakesolutions.kafka.akka

import cakesolutions.kafka.KafkaProducerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeTag

object KafkaIngestible {
  def fromValues[Value: TypeTag](
    topic: String,
    values: Seq[Value],
    response: Option[Any]): KafkaIngestible[Nothing, Value] = {

    val records = values.map(value => KafkaProducerRecord(topic, value))
    KafkaIngestible[Nothing, Value](records, response)
  }

  def fromValuesWithKey[Key: TypeTag, Value: TypeTag](
    topic: String,
    key: Key,
    values: Seq[Value],
    response: Option[Any]): KafkaIngestible[Key, Value] = {

    val records = values.map(value => KafkaProducerRecord(topic, key, value))
    KafkaIngestible(records, response)
  }

  def fromValuesWithTopic[Value: TypeTag](
    valuesWithTopic: Seq[(String, Value)],
    response: Option[Any]): KafkaIngestible[Nothing, Value] = {

    val records = valuesWithTopic.map {
      case (topic, value) => KafkaProducerRecord(topic, value)
    }
    KafkaIngestible[Nothing, Value](records, response)
  }

  def fromKeyValues[Key: TypeTag, Value: TypeTag](
    topic: String,
    keyValues: Seq[(Key, Value)],
    response: Option[Any]): KafkaIngestible[Key, Value] = {

    val records = keyValues.map {
      case (key, value) => KafkaProducerRecord(topic, key, value)
    }
    KafkaIngestible(records, response)
  }

  def fromKeyValues[Key: TypeTag, Value: TypeTag](
    topic: String,
    keyValues: KeyValues[Key, Value],
    response: Option[Any]): KafkaIngestible[Key, Value] = {

    KafkaIngestible(keyValues.toProducerRecords(topic), response)
  }

  def fromKeyValues[Key: TypeTag, Value: TypeTag](
    topic: String,
    keyValuesWithOffsets: KeyValuesWithOffsets[Key, Value]): KafkaIngestible[Key, Value] = {

    KafkaIngestible(
      keyValuesWithOffsets.keyValues.toProducerRecords(topic),
      Some(keyValuesWithOffsets.offsets)
    )
  }

  def fromKeyValuesWithTopic[Key: TypeTag, Value: TypeTag](
    keyValuesWithTopic: Seq[(String, Key, Value)],
    response: Option[Any]): KafkaIngestible[Key, Value] = {

    val records = keyValuesWithTopic.map {
      case (topic, key, value) => KafkaProducerRecord(topic, key, value)
    }
    KafkaIngestible(records, response)
  }

  /**
    * Create an extractor for pattern matching any value with a specific [[KeyValues]] type.
    *
    * @tparam Key the type of the key to match in the extractor
    * @tparam Value the type of the value to match in the extractor
    * @return an extractor for given [[Key]] and [[Value]]
    */
  def extractor[Key: TypeTag, Value: TypeTag]: Extractor[Key, Value] = new Extractor[Key, Value]

  /**
    * Extractor for pattern matching any value with a specific [[KafkaIngestible]] type.
    *
    * @tparam Key the type of the key to match in the extractor
    * @tparam Value the type of the value to match in the extractor
    */
  class Extractor[Key: TypeTag, Value: TypeTag] {
    def unapply(a: Any): Option[KafkaIngestible[Key, Value]] = a match {
      case ingestible: KafkaIngestible[_, _] => ingestible.cast[Key, Value]
      case _ => None
    }
  }
}

case class KafkaIngestible[Key: TypeTag, Value: TypeTag](
  records: Iterable[ProducerRecord[Key, Value]],
  response: Option[Any]) {

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
  def cast[OtherKey: TypeTag, OtherValue: TypeTag]: Option[KafkaIngestible[OtherKey, OtherValue]] =
    if (hasType[OtherKey, OtherValue]) Some(this.asInstanceOf[KafkaIngestible[OtherKey, OtherValue]])
    else None
}