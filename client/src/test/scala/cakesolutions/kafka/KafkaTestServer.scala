package cakesolutions.kafka

import cakesolutions.kafka.testkit.KafkaServer
import org.scalatest.{FlatSpecLike, BeforeAndAfterAll, FlatSpec, Matchers}

trait KafkaTestServer extends FlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
