package com.pirum

import com.pirum.testkit.KafkaServer
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class KafkaIntSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()
  val kafkaPort = kafkaServer.kafkaPort

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}
