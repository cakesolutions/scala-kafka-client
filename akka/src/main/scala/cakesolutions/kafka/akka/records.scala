package cakesolutions.kafka.akka

import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.reflect.runtime.universe._
import scala.collection.JavaConversions._

object Offsets {
  def empty: Offsets = Offsets(Map.empty)
}

/**
  * Map of partitions to partition offsets.
  */
case class Offsets(offsetsMap: Map[TopicPartition, Long]) extends AnyVal {
  def get(topic: TopicPartition): Option[Long] = offsetsMap.get(topic)

  def forAllOffsets(that: Offsets)(f: (Long, Long) => Boolean): Boolean =
    offsetsMap.forall {
      case (topic, offset) => that.get(topic).forall(f(offset, _))
    }

  def toCommitMap: Map[TopicPartition, OffsetAndMetadata] =
    offsetsMap.mapValues(offset => new OffsetAndMetadata(offset))

  override def toString: String =
    offsetsMap
      .map { case (t, o) => s"$t: $o" }
      .mkString("Offsets(", ", ", ")")
}

trait HasOffsets {
  /**
    * The offsets assigned to the client after the records were pulled from Kafka.
    */
  val offsets: Offsets
}

object ValueRecords {
  def apply[Value: TypeTag](offsets: Offsets, values: Seq[Value]): ValueRecords[Value] =
    PlainValueRecords(offsets, values)

  def extractor[Value: TypeTag]: Extractor[Value] = new Extractor[Value]

  class Extractor[Value: TypeTag] {
    def unapply(a: Any): Option[(Offsets, Seq[Value])] = a match {
      case rs: ValueRecords[_] => rs.cast[Value].map(_.toTuple)
      case _ => None
    }
  }
}

sealed abstract class ValueRecords[Value: TypeTag] extends HasOffsets {
  val valueTag: TypeTag[Value] = typeTag[Value]

  /**
    * Compare given type to record type.
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherValue the value type to compare to
    * @return true when given types match objects type parameters, and false otherwise
    */
  def hasType[OtherValue: TypeTag]: Boolean = typeTag[OtherValue].tpe <:< valueTag.tpe

  /**
    * Attempt to cast record values to given types.
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherValue the value type to cast to
    * @return the same records in casted form when casting is possible, and otherwise [[None]]
    */
  def cast[OtherValue: TypeTag]: Option[ValueRecords[OtherValue]] =
    if (hasType[OtherValue]) Some(this.asInstanceOf[ValueRecords[OtherValue]])
    else None

  /**
    * All the values that the records contain
    */
  def values: Seq[Value]

  def toTuple: (Offsets, Seq[Value]) = (offsets, values)
}

case class PlainValueRecords[Value: TypeTag](offsets: Offsets, values: Seq[Value]) extends ValueRecords[Value]

object KeyValueRecords {
  def apply[Key: TypeTag, Value: TypeTag](offsets: Offsets, keyValues: Seq[(Key, Value)]): KeyValueRecords[Key, Value] =
    PlainKeyValueRecords(offsets, keyValues)

  class Extractor[Key: TypeTag, Value: TypeTag] {
    def unapply(a: Any): Option[(Offsets, Seq[(Key, Value)])] = a match {
      case rs: KeyValueRecords[_, _] => rs.cast[Key, Value].map(_.toTuple)
      case _ => None
    }
  }
}

sealed abstract class KeyValueRecords[Key: TypeTag, Value: TypeTag] extends HasOffsets {
  val keyTag: TypeTag[Key] = typeTag[Key]
  val valueTag: TypeTag[Value] = typeTag[Value]

  /**
    * Compare given types to record types.
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherKey the key type to compare to
    * @tparam OtherValue the value type to compare to
    * @return true when given types match objects type parameters, and false otherwise
    */
  def hasType[OtherKey: TypeTag, OtherValue: TypeTag]: Boolean =
    typeTag[OtherKey].tpe <:< keyTag.tpe && typeTag[OtherValue].tpe <:< valueTag.tpe

  /**
    * Attempt to cast record keys and values to given types.
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @tparam OtherKey the key type to cast to
    * @tparam OtherValue the value type to cast to
    * @return the same records in casted form when casting is possible, and otherwise [[None]]
    */
  def cast[OtherKey: TypeTag, OtherValue: TypeTag]: Option[KeyValueRecords[OtherKey, OtherValue]] =
    if (hasType[OtherKey, OtherValue]) Some(this.asInstanceOf[KeyValueRecords[OtherKey, OtherValue]])
    else None

  /**
    * Drop keys and convert to just values
    */
  def toValueRecords: ValueRecords[Value] = PlainValueRecords(offsets, values)

  /**
    * All the values that the records contain
    */
  def values: Seq[Value] = keyValues.map(_._2)

  /**
    * All the keys and values that the records contain
    */
  def keyValues: Seq[(Key, Value)]

  def toTuple: (Offsets, Seq[(Key, Value)]) = (offsets, keyValues)
}

case class PlainKeyValueRecords[Key: TypeTag, Value: TypeTag](offsets: Offsets, keyValues: Seq[(Key, Value)])
  extends KeyValueRecords[Key, Value]

/**
  * Records consumed from Kafka with the offsets related to the records.
  * This allows accessing the full Kafka record information.
  */
case class FullRecords[Key: TypeTag, Value: TypeTag](offsets: Offsets, records: ConsumerRecords[Key, Value])
  extends KeyValueRecords[Key, Value] {

  lazy val recordsList = records.toList

  override def keyValues: Seq[(Key, Value)] = recordsList.map(r => (r.key(), r.value()))

  override def values: Seq[Value] = recordsList.map(_.value())
}
