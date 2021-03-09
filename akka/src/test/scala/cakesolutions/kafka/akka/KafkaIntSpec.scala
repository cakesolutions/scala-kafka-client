package com.pirum.akka

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.pirum.testkit.KafkaServer
import org.scalatest.concurrent.Waiters
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

/** ScalaTest base class for scala-kafka-client-testkit based integration tests
  */
class KafkaIntSpec(_system: ActorSystem)
    extends TestKit(_system)
    with Waiters
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  val kafkaServer = new KafkaServer()
  val kafkaPort = kafkaServer.kafkaPort

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
    TestKit.shutdownActorSystem(system)
  }
}
