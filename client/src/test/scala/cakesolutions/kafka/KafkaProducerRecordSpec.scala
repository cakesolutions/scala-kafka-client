package cakesolutions.kafka

import java.nio.ByteBuffer
import java.util.UUID

import cakesolutions.kafka.KafkaProducerRecord.Destination
import org.apache.kafka.common.header.internals.RecordHeader
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class KafkaProducerRecordSpec extends AnyFlatSpecLike with Matchers {
  private val headers = Seq(
    new RecordHeader("foo", "kafka".getBytes),
    new RecordHeader("bar", ByteBuffer.wrap("apache".getBytes))
  )
  type K = String
  type V = UUID
  private val topic = "scala-kafka-client"
  private val k = "west"
  private val v = UUID.randomUUID()
  private val dest = Destination(topic = topic)

  "apply[Key >: Null, Value](topic: String, key: Option[Key], value: Value)" should "create the specified record" in {
    val rec = KafkaProducerRecord(topic, Some(k), v)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe Seq.empty
  }

  "apply[Key >: Null, Value](topic: String, key: Option[Key], value: Value, headers: Seq[Header])" should "create the specified record" in {
    val rec = KafkaProducerRecord(topic, Some(k), v, headers)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe headers
  }

  "apply[Key, Value](topic: String, key: Key, value: Value)" should "create the specified record" in {
    val rec = KafkaProducerRecord(topic, k, v)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe Seq.empty
  }

  "apply[Key, Value](topic: String, key: Key, value: Value, headers: Seq[Header])" should "create the specified record" in {
    val rec = KafkaProducerRecord(topic, k, v, headers)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe headers
  }

  "apply[Key >: Null, Value](topic: String, value: Value)" should "create the specified record" in {
    val rec = KafkaProducerRecord[K, V](topic, v)
    rec.topic() shouldBe topic
    rec.key() shouldBe null
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe Seq.empty
  }

  "apply[Key >: Null, Value](topic: String, value: Value, headers: Seq[Header])" should "create the specified record" in {
    val rec = KafkaProducerRecord[K, V](topic, v, headers)
    rec.topic() shouldBe topic
    rec.key() shouldBe null
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe headers
  }

  "apply[Key >: Null, Value](topicPartitionSelection: Destination, key: Option[Key] = None, value: Value, timestamp: Option[Long] = None)" should "create the specified record" in {
    val rec = KafkaProducerRecord(dest, Some(k), v)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe Seq.empty
  }

  "apply[Key >: Null, Value](topicPartitionSelection: Destination, key: Option[Key] = None, value: Value, timestamp: Option[Long] = None, headers: Seq[Header])" should "create the specified record" in {
    val rec = KafkaProducerRecord(dest, Some(k), v, timestamp = None, headers)
    rec.topic() shouldBe topic
    rec.key() shouldBe k
    rec.value() shouldBe v
    rec.headers().asScala.toSeq shouldBe headers
  }
}
