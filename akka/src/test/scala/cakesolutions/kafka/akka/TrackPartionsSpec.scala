package com.pirum.akka

import java.util.{Collection => JCollection}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.pirum.KafkaConsumer
import com.pirum.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{
  ConsumerRebalanceListener,
  ConsumerRecord,
  OffsetResetStrategy,
  ConsumerRecords => JConsumerRecords,
  KafkaConsumer => JKafkaConsumer
}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

class TrackPartionsSpec(system_ : ActorSystem)
    extends TestKit(system_)
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  def this() = this(ActorSystem("KafkaConsumerActorSpec"))

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val kafkaPort = 80
  val topic = "foo"
  val partition = 0
  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "$randomString"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin),
      new StringDeserializer,
      new StringDeserializer
    )
  }

  def consumerConf: KafkaConsumer.Conf[String, String] =
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:$kafkaPort",
      groupId = randomString,
      enableAutoCommit = false,
      autoOffsetReset = OffsetResetStrategy.EARLIEST
    )

  def actorConfFromConfig: KafkaConsumerActor.Conf =
    KafkaConsumerActor.Conf(ConfigFactory.parseString(s"""
         | schedule.interval = 1 second
         | unconfirmed.timeout = 3 seconds
         | max.redeliveries = 3
        """.stripMargin))

  def configuredActor: Config =
    ConfigFactory.parseString(
      s"""
         | bootstrap.servers = "localhost:$kafkaPort",
         | group.id = "$randomString"
         | enable.auto.commit = false
         | auto.offset.reset = "earliest"
         | schedule.interval = 1 second
         | unconfirmed.timeout = 3 seconds
         | max.redeliveries = 3
        """.stripMargin
    )

  Seq(
    (Subscribe.AutoPartition(Seq(topic)), "AutoPartition"),
    (Subscribe.AutoPartitionBasic(Seq(topic)), "AutoPartitionBasic")
  ).foreach { case (subType, label) =>
    s"KafkaConsumerActor configured in $label mode" should "not seek to stale offset when re-assigned same partition after another consumer had partition" in {
      val kConsumer = mock[JKafkaConsumer[String, String]]
      val listenerCaptor =
        ArgumentCaptor.forClass(classOf[ConsumerRebalanceListener])
      val consumerActor = system.actorOf(
        KafkaConsumerActor.props(
          consumerConf,
          KafkaConsumerActor.Conf(),
          testActor,
          kConsumer
        )
      )

      val tp = new TopicPartition(topic, partition)
      val recList =
        List(new ConsumerRecord(topic, partition, 0, "1", "foo")).asJava
      val recMap =
        new JConsumerRecords[String, String](Map(tp -> recList).asJava)
      val recMapEmpty = JConsumerRecords.empty[String, String]()

      doNothing()
        .when(kConsumer)
        .subscribe(any[JCollection[String]](), listenerCaptor.capture())
      when(kConsumer.poll(anyLong()))
        .thenReturn(recMap)
        .thenReturn(recMapEmpty)
      when(kConsumer.position(tp)).thenReturn(recMap.count())

      consumerActor ! subType

      Thread.sleep(1000)
      verify(kConsumer).subscribe(
        any[JCollection[String]](),
        any[ConsumerRebalanceListener]()
      )
      val listener: ConsumerRebalanceListener = listenerCaptor.getValue
      listener.onPartitionsAssigned(List(tp).asJava)
      val rs =
        expectMsgClass(5.seconds, classOf[ConsumerRecords[String, String]])

      consumerActor ! Confirm(rs.offsets, true)

      listener.onPartitionsRevoked(List(tp).asJava)
      listener.onPartitionsAssigned(List.empty.asJava)
      listener.onPartitionsRevoked(List.empty.asJava)
      listener.onPartitionsAssigned(List(tp).asJava)
      verify(kConsumer, never).seek(any[TopicPartition](), any[Long]())
    }
  }
}
