package cakesolutions.kafka

import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Helper functions for creating Kafka's [[ProducerRecord]]s.
  *
  * The producer records hold the data that is to be written to Kafka.
  * The producer records are compatible with Kafka's own `KafkaProducer` and the [[KafkaProducer]] in this library.
  */
object KafkaProducerRecord {

  /**
    * Create a producer record with an optional key.
    *
    * @param topic topic to which record is being sent
    * @param key optional key that will be included in the record
    * @param value the value that will be included in the record
    * @tparam K type of the key
    * @tparam V type of the value
    * @return producer records
    */
  def apply[K, V](topic: String, key: Option[K], value: V): ProducerRecord[K, V] =
    key match {
      case Some(k) => new ProducerRecord(topic, k, value)
      case None => new ProducerRecord(topic, value)
    }

  /**
    * Create a producer record with topic, key, and value.
    *
    * @param topic topic to which record is being sent
    * @param key the key that will be included in the record
    * @param value the value that will be included in the record
    * @tparam K type of the key
    * @tparam V type of the value
    * @return producer records
    */
  def apply[K, V](topic: String, key: K, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, key, value)

  /**
    * Create a producer record without a key.
    *
    * @param topic topic to which record is being sent
    * @param value the value that will be included in the record
    * @tparam K type of the key
    * @tparam V type of the value
    * @return producer records
    */
  def apply[K, V](topic: String, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, value)
}
