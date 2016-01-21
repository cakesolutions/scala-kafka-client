package cakesolutions.kafka

import org.apache.kafka.clients.producer.ProducerRecord

object KafkaProducerRecord {
  def apply[K, V](topic: String, key: Option[K], value: V): ProducerRecord[K, V] =
    key match {
      case Some(k) => new ProducerRecord(topic, k, value)
      case None => new ProducerRecord(topic, value)
    }

  def apply[K, V](topic: String, key: K, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, key, value)

  def apply[K, V](topic: String, value: V): ProducerRecord[K, V] =
    new ProducerRecord(topic, value)
}
