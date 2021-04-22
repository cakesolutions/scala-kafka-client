package com.pirum.kafka

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}

object KafkaDeserializer {

  /** Creates a Kafka Deserializer from a supplied function.
    * @param f function from Array[Byte] to the Deserialized type.
    * @tparam T The type to deserialize to.
    * @return Kafka Deserializer that can be passed to the KafkaConsumer constructor.
    */
  def apply[T](f: Array[Byte] => T): Deserializer[T] = new FunDeserializer(f)
}

object KafkaSerializer {

  /** Creates a Kafka Serializer from a function.
    * @param f function from serialized type to Array[Byte]
    * @tparam T The type to serialize from.
    * @return Kafka Serializer that can be passed to the KafkaConsumer constructor.
    */
  def apply[T](f: T => Array[Byte]): Serializer[T] = new FunSerializer[T](f)
}

/** Transforms the ``f`` to be a ``Deserializer[A]``.
  *
  * @param f the function that (statelessly) performs the deserialization
  */
private class FunDeserializer[T](f: Array[Byte] ⇒ T) extends Deserializer[T] {

  override def configure(
      configs: util.Map[String, _],
      isKey: Boolean
  ): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = f(data)
}

/** Transforms the ``f`` to be a ``Serializer[A]``.
  *
  * @param f the function that (statelessly) performs the serialization
  */
private class FunSerializer[T](f: T => Array[Byte]) extends Serializer[T] {

  override def configure(
      configs: util.Map[String, _],
      isKey: Boolean
  ): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = f(data)

  override def close(): Unit = {}
}
