package com.pirum

import java.util
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

class ConfigureSerializationSpec extends KafkaIntSpec {

  private class MockDeserializer() extends Deserializer[String] {
    var configuration: String = _
    var isKeyDeserializer: Boolean = _

    override def configure(
        configs: util.Map[String, _],
        isKey: Boolean
    ): Unit = {
      configuration = configs.get("mock.config").toString
      isKeyDeserializer = isKey
    }

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): String =
      new String(data)
  }

  private class MockSerializer() extends Serializer[String] {
    var configuration: String = _
    var isKeySerializer: Boolean = _

    override def configure(
        configs: util.Map[String, _],
        isKey: Boolean
    ): Unit = {
      configuration = configs.get("mock.config").toString
      isKeySerializer = isKey
    }

    override def serialize(topic: String, data: String): Array[Byte] =
      data.getBytes

    override def close(): Unit = {}
  }

  "Producer" should "configure the serializers" in {
    val keySerializer = new MockSerializer
    val valueSerializer = new MockSerializer

    val conf = KafkaProducer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | mock.config = "mock_value"
         """.stripMargin
      ),
      keySerializer,
      valueSerializer
    )

    val producer = KafkaProducer(conf)
    producer.close

    keySerializer.configuration shouldEqual "mock_value"
    keySerializer.isKeySerializer shouldEqual true
    valueSerializer.configuration shouldEqual "mock_value"
    valueSerializer.isKeySerializer shouldEqual false
  }

  "Consumer" should "configure the deserializers" in {
    val keyDeserializer = new MockDeserializer
    val valueDeserializer = new MockDeserializer

    val conf = KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | mock.config = "mock_value"
         """.stripMargin
      ),
      keyDeserializer,
      valueDeserializer
    )

    val consumer = KafkaConsumer(conf)
    consumer.close

    keyDeserializer.configuration shouldEqual "mock_value"
    keyDeserializer.isKeyDeserializer shouldEqual true
    valueDeserializer.configuration shouldEqual "mock_value"
    valueDeserializer.isKeyDeserializer shouldEqual false
  }
}
