package cakesolutions.kafka.akka

import cakesolutions.kafka.KafkaProducerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * Helper functions for [[ProducerRecords]].
  */
object ProducerRecords {

  /**
    * Create producer records from a sequence of values.
    * All the records will have the given topic and no key.
    *
    * @param topic topic to write the records to
    * @param values values of the records
    * @param successResponse optional response message to the sender on successful delivery
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromValues[Value: TypeTag](
    topic: String,
    values: Seq[Value],
    successResponse: Option[Any],
    failureResponse: Option[Any]
  ): ProducerRecords[Nothing, Value] = {
    val records = values.map(value => KafkaProducerRecord(topic, value))
    ProducerRecords[Nothing, Value](records, successResponse, failureResponse)
  }

  /**
    * Create producer records from a single key and multiple values.
    * All the records will have the given topic and key.
    *
    * @param topic topic to write the records to
    * @param key key of the records
    * @param values values of the records
    * @param successResponse optional response message to the sender on successful delivery
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromValuesWithKey[Key: TypeTag, Value: TypeTag](
    topic: String,
    key: Key,
    values: Seq[Value],
    successResponse: Option[Any],
    failureResponse: Option[Any]
  ): ProducerRecords[Key, Value] = {

    val records = values.map(value => KafkaProducerRecord(topic, key, value))
    ProducerRecords(records, successResponse, failureResponse)
  }

  /**
    * Create producer records from topics and values.
    * All the records will have no key.
    *
    * @param valuesWithTopic a sequence of topic and value pairs
    * @param successResponse optional response message to the sender on successful delivery
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromValuesWithTopic[Value: TypeTag](
    valuesWithTopic: Seq[(String, Value)],
    successResponse: Option[Any],
    failureResponse: Option[Any]
  ): ProducerRecords[Nothing, Value] = {

    val records = valuesWithTopic.map {
      case (topic, value) => KafkaProducerRecord(topic, value)
    }
    ProducerRecords[Nothing, Value](records, successResponse)
  }

  /**
    * Create producer records from key-value pairs.
    * All the records will have the given topic.
    *
    * @param topic topic to write the records to
    * @param keyValues a sequence of key and value pairs
    * @param successResponse optional response message to the sender on successful delivery
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromKeyValues[Key: TypeTag, Value: TypeTag](
    topic: String,
    keyValues: Seq[(Key, Value)],
    successResponse: Option[Any],
    failureResponse: Option[Any]
  ): ProducerRecords[Key, Value] = {

    val records = keyValues.map {
      case (key, value) => KafkaProducerRecord(topic, key, value)
    }
    ProducerRecords(records, successResponse, failureResponse)
  }

  /**
    * Create producer records from topic, key, and value triples.
    *
    * @param keyValuesWithTopic a sequence of topic, key, and value triples.
    * @param successResponse optional response message to the sender on successful delivery
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromKeyValuesWithTopic[Key: TypeTag, Value: TypeTag](
    keyValuesWithTopic: Seq[(String, Key, Value)],
    successResponse: Option[Any],
    failureResponse: Option[Any]
  ): ProducerRecords[Key, Value] = {

    val records = keyValuesWithTopic.map {
      case (topic, key, value) => KafkaProducerRecord(topic, key, value)
    }
    ProducerRecords(records, successResponse, failureResponse)
  }

  /**
    * Convert consumer records to key-value pairs.
    * All the records will have the given topic, and the offsets are used as the response message.
    *
    * @param topic topic to write the records to
    * @param consumerRecords records from [[KafkaConsumerActor]]
    * @param failureResponse optional response message to the sender on failed delivery
    */
  def fromConsumerRecords[Key: TypeTag, Value: TypeTag](
    topic: String,
    consumerRecords: ConsumerRecords[Key, Value],
    failureResponse: Option[Any]
  ) =
    ProducerRecords(
      consumerRecords.toProducerRecords(topic),
      Some(consumerRecords.offsets),
      Some(failureResponse)
    )

  /**
    * Create an extractor for pattern matching any value with a specific [[ProducerRecords]] type.
    *
    * @tparam Key the type of the key to match in the extractor
    * @tparam Value the type of the value to match in the extractor
    * @return an extractor for given key and value types
    */
  def extractor[Key: TypeTag, Value: TypeTag]: Extractor[Any, ProducerRecords[Key, Value]] =
    Extractor {
      case producerRecords: ProducerRecords[_, _] => producerRecords.cast[Key, Value]
      case _ => None
    }
}

/**
  * Records that [[KafkaProducerActor]] can write to Kafka.
  *
  * @param records the records that are to be written to Kafka
  * @param successResponse an optional message that is to be sent back to the sender after records have been successfully written to Kafka
  * @param failureResponse optional message that is to be sent back to the sender when messages were not successfully written to Kafka
  */
case class ProducerRecords[Key: TypeTag, Value: TypeTag](
  records: Iterable[ProducerRecord[Key, Value]],
  successResponse: Option[Any] = None,
  failureResponse: Option[Any] = None
) {

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
  def cast[OtherKey: TypeTag, OtherValue: TypeTag]: Option[ProducerRecords[OtherKey, OtherValue]] =
    if (hasType[OtherKey, OtherValue]) Some(this.asInstanceOf[ProducerRecords[OtherKey, OtherValue]])
    else None
}
