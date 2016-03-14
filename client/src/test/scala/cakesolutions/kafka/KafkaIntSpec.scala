package cakesolutions.kafka

import cakesolutions.kafka.testkit.KafkaServer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class KafkaTestSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()
  val kafkaPort = kafkaServer.kafkaPort

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
