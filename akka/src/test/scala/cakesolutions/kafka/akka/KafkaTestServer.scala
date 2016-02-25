package cakesolutions.kafka.akka

import cakesolutions.kafka.testkit.KafkaServer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

//TODO duplication
trait KafkaTestServer extends FlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
