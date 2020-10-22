package cakesolutions.kafka

import cakesolutions.kafka.testkit.KafkaServer
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class KafkaIntSpec extends AnyFlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()
  val kafkaPort = kafkaServer.kafkaPort

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
