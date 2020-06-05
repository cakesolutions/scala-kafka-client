package cakesolutions.kafka

import cakesolutions.kafka.KafkaTopicPartition.{Partition, Topic}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import scala.collection.JavaConverters._

/**
  * Helper functions for creating Kafka's `ProducerRecord`s.
  *
  * The producer records hold the data that is to be written to Kafka.
  * The producer records are compatible with Kafka's own `KafkaProducer` and the [[KafkaProducer]] in this library.
  */
object KafkaProducerRecord {

  /**
    * Destination for Kafka producer records.
    */
  object Destination {
    /**
      * Destination by topic.
      *
      * Selects the destination for producer records by topic only.
      */
    def apply(topic: Topic): Destination = Destination(topic, None)

    /**
      * Destination by topic and partition.
      *
      * Selects the destination for producer records to a specific topic and partition.
      */
    def apply(topic: Topic, partition: Partition): Destination = Destination(topic, Some(partition))

    /**
      * Destination by topic and partition.
      *
      * Selects the destination for producer records to a specific topic and partition.
      */
    def apply(topicPartition: TopicPartition): Destination = Destination(topicPartition.topic(), topicPartition.partition())
  }

  /**
    * Destination for Kafka producer records.
    */
  final case class Destination(topic: Topic, partition: Option[Partition])

  /**
    * Create a producer record with an optional key.
    *
    * @param topic the topic where the record will be appended to
    * @param key optional key that will be included in the record
    * @param value the value that will be included in the record
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key, Value](topic: String, key: Option[Key], value: Value): ProducerRecord[Key, Value] =
    key match {
      case Some(k) => new ProducerRecord(topic, k, value)
      case None => new ProducerRecord(topic, value)
    }

  /**
    * Create a producer record with an optional key.
    *
    * @param topic the topic where the record will be appended to
    * @param key optional key that will be included in the record
    * @param value the value that will be included in the record
    * @param headers the record headers
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
    def apply[Key >: Null, Value](topic: String, key: Option[Key], value: Value,
                          headers: Seq[Header]): ProducerRecord[Key, Value] =
      key match {
        case Some(k) => apply(topic, k, value, headers)
        case None => apply(topic, value, headers)
      }

  /**
    * Create a producer record with topic, key, and value.
    *
    * @param topic the topic where the record will be appended to
    * @param key the key that will be included in the record
    * @param value the value that will be included in the record
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key, Value](topic: String, key: Key, value: Value): ProducerRecord[Key, Value] =
    new ProducerRecord(topic, key, value)

  /**
    * Create a producer record with topic, key, and value.
    *
    * @param topic the topic where the record will be appended to
    * @param key the key that will be included in the record
    * @param value the value that will be included in the record
    * @param headers the record headers
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key, Value](topic: String, key: Key, value: Value,
                        headers: Seq[Header]): ProducerRecord[Key, Value] =
    new ProducerRecord(topic, null, key, value, headers.asJava)

  /**
    * Create a producer record without a key.
    *
    * @param topic topic to which record is being sent
    * @param value the value that will be included in the record
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key, Value](topic: String, value: Value): ProducerRecord[Key, Value] =
    new ProducerRecord(topic, value)

  /**
    * Create a producer record without a key.
    *
    * @param topic topic to which record is being sent
    * @param value the value that will be included in the record
    * @param headers the record headers
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key >: Null, Value](topic: String, value: Value,
                        headers: Seq[Header]): ProducerRecord[Key, Value] =
    new ProducerRecord(topic, null, null, null, value, headers.asJava)

  /**
    * Create a producer record from a topic selection, optional key, value, and optional timestamp.
    * 
    * @param topicPartitionSelection the topic (with optional partition) where the record will be appended to
    * @param key the key that will be included in the record
    * @param value the value that will be included in the record
    * @param timestamp the timestamp of the record
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key >: Null, Value](
    topicPartitionSelection: Destination,
    key: Option[Key] = None,
    value: Value,
    timestamp: Option[Long] = None
  ): ProducerRecord[Key, Value] =
    apply(topicPartitionSelection, key, value, timestamp, Seq.empty)

  /**
    * Create a producer record from a topic selection, optional key, value, and optional timestamp.
    *
    * @param topicPartitionSelection the topic (with optional partition) where the record will be appended to
    * @param key the key that will be included in the record
    * @param value the value that will be included in the record
    * @param timestamp the timestamp of the record
    * @param headers the record headers
    * @tparam Key type of the key
    * @tparam Value type of the value
    * @return producer record
    */
  def apply[Key >: Null, Value](
                                 topicPartitionSelection: Destination,
                                 key: Option[Key],
                                 value: Value,
                                 timestamp: Option[Long],
                                 headers: Seq[Header]
                               ): ProducerRecord[Key, Value] = {
    val topic = topicPartitionSelection.topic
    val partition = topicPartitionSelection.partition.map(i => i: java.lang.Integer).orNull
    val nullableKey = key.orNull
    val nullableTimestamp = timestamp.map(i => i: java.lang.Long).orNull

    new ProducerRecord[Key, Value](topic, partition, nullableTimestamp, nullableKey, value, headers.asJava)
  }

  /**
    * Create producer records from a sequence of values.
    * All the records will have the given topic and no key.
    *
    * @param topic topic to write the records to
    * @param values values of the records
    */
  def fromValues[Value](topic: String, values: Seq[Value]): Seq[ProducerRecord[Nothing, Value]] =
    values.map(value => KafkaProducerRecord(topic, value))

  /**
    * Create producer records from a single key and multiple values.
    * All the records will have the given topic and key.
    *
    * @param topic topic to write the records to
    * @param key key of the records
    * @param values values of the records
    */
  def fromValuesWithKey[Key, Value](topic: String, key: Option[Key], values: Seq[Value]): Seq[ProducerRecord[Key, Value]] =
    values.map(value => KafkaProducerRecord(topic, key, value))

  /**
    * Create producer records from topics and values.
    * All the records will have no key.
    *
    * @param valuesWithTopic a sequence of topic and value pairs
    */
  def fromValuesWithTopic[Value](valuesWithTopic: Seq[(String, Value)]): Seq[ProducerRecord[Nothing, Value]] =
    valuesWithTopic.map {
      case (topic, value) => KafkaProducerRecord(topic, value)
    }

  /**
    * Create producer records from key-value pairs.
    * All the records will have the given topic.
    *
    * @param topic topic to write the records to
    * @param keyValues a sequence of key and value pairs
    */
  def fromKeyValues[Key, Value](topic: String, keyValues: Seq[(Option[Key], Value)]): Seq[ProducerRecord[Key, Value]] =
    keyValues.map {
      case (key, value) => KafkaProducerRecord(topic, key, value)
    }

  /**
    * Create producer records from topic, key, and value triples.
    *
    * @param keyValuesWithTopic a sequence of topic, key, and value triples.
    */
  def fromKeyValuesWithTopic[Key, Value](keyValuesWithTopic: Iterable[(String, Option[Key], Value)]): Iterable[ProducerRecord[Key, Value]] =
    keyValuesWithTopic.map {
      case (topic, key, value) => KafkaProducerRecord(topic, key, value)
    }
}
