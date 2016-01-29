package cakesolutions

import cakesolutions.kafka.testkit.KafkaServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

trait KafkaTestServer extends FlatSpec with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
