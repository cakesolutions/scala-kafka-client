package cakesolutions.kafka.akka

import java.time.Duration
import java.util.{Collection => JCollection, HashMap => JHashMap, List => JList}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cakesolutions.kafka.KafkaConsumer
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, ConsumerRecord, OffsetResetStrategy, ConsumerRecords => JConsumerRecords, KafkaConsumer => JKafkaConsumer}
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


class TrackPartionsSpec(system_ : ActorSystem) extends TestKit(system_)   with FlatSpecLike
  with Matchers
  with BeforeAndAfterAll with MockitoSugar {

  def this() = this(ActorSystem("KafkaConsumerActorSpec"))

  private def randomString: String = Random.alphanumeric.take(5).mkString("")

  val kafkaPort = 80
  val topic = "foo"
  val partition = 0
  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:$kafkaPort",
           | group.id = "$randomString"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)
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
    KafkaConsumerActor.Conf(ConfigFactory.parseString(
      s"""
         | schedule.interval = 1 second
         | unconfirmed.timeout = 3 seconds
         | max.redeliveries = 3
        """.stripMargin)
    )

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

  "KafkaConsumerActor configured in Auto Partition mode" should "???" in {
    val kConsumer = mock[JKafkaConsumer[String, String]]
    val lc = ArgumentCaptor.forClass(classOf[ConsumerRebalanceListener])
    doNothing().when(kConsumer).subscribe(any[JCollection[String]](), lc.capture())

    val tp = new TopicPartition(topic, partition)
    val recs = List(
      new ConsumerRecord(topic, partition, 0, "1", "foo")
    ).asJava
    val recs2 = new JConsumerRecords[String, String](
      Map(tp -> recs).asJava,
    )
    val noRecs = JConsumerRecords.empty[String, String]()

    when(kConsumer.poll(anyLong()))
      .thenReturn(recs2)
      .thenReturn(noRecs)

    val consumerActor = system.actorOf(KafkaConsumerActor.props(consumerConf, KafkaConsumerActor.Conf(), testActor, kConsumer))
    consumerActor ! Subscribe.AutoPartition(Seq("foo"))
    val rs = expectMsgClass(30.seconds, classOf[ConsumerRecords[String, String]])
    val l: ConsumerRebalanceListener = lc.getValue
    l.onPartitionsAssigned(List(tp).asJava)
    verify(kConsumer).subscribe(any[JCollection[String]](), any[ConsumerRebalanceListener]())

    consumerActor ! Confirm(rs.offsets, true)
    Thread.sleep(1000)

    when(kConsumer.position(tp)).thenReturn(
      recs2.count(),
      )
    l.onPartitionsRevoked(List(tp).asJava)
    l.onPartitionsAssigned(List.empty.asJava)
    l.onPartitionsAssigned(List(tp).asJava)
    verify(kConsumer).seek(any[TopicPartition](), any[Long]())
  }
}
