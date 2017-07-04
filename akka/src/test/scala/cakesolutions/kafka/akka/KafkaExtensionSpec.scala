package cakesolutions.kafka.akka

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

class KafkaExtensionSpec(system_ : ActorSystem) extends KafkaIntSpec(system_) {

  def this() = this(ActorSystem("test",
    ConfigFactory.parseString(
      """
        |akka.extensions = ["cakesolutions.kafka.akka.KafkaExtension"]
        |app {
        |  kafka {
        |    consumer-config {
        |      bootstrap.servers = "localhost:9092"
        |      group.id = foo
        |      auto.offset.reset = "earliest"
        |    }
        |
        |    producer-config {
        |      bootstrap.servers = "localhost:9092"
        |    }
        |
        |  }
        |}
      """.stripMargin)))

  it should "correctly handle multiple producers" in {
    val producers = List.fill(10)(
      KafkaExtension(system).actorProducer(ActorRef.noSender).build(new StringSerializer, new StringSerializer)
    )

    producers.forall(_ == producers.head) shouldBe true
  }

  it should "subscribe and receive messages using actor & future APIs" in {
    val ap = KafkaExtension(system)
      .actorProducer(ActorRef.noSender)
      .build(new StringSerializer, new StringSerializer)
    val fp = KafkaExtension(system)
      .futureProducer()
      .build(new StringSerializer, new StringSerializer)

    // Create the KafkaSubscriberActor that demonstrates the usage of the KafkaExtension
    val sa = TestActorRef[KafkaSubscriberActor]

    // Wait for everything to settle down...
    Thread.sleep(1000)

    // We produce 2 messages
    ap.produce("test", "k", "a", ())
    ap.produce("test", "k", "b", ())

    import system.dispatcher
    fp.produce("test", "k", "aaaaa")
    fp.produce("test", "k", "bbbbb")

    // Wait for everything to settle down...
    Thread.sleep(5000)

    // and we expect to consume them
    sa.underlyingActor.received should contain allElementsOf List("k:a", "k:b", "k:aaaaa", "k:bbbbb")

    system.stop(sa)
  }

}

/**
  * This actor demonstrates the usage of the basic consumer and basic producer.
  *
  * The meaning of "basic" for consumers depends on the subscription mode: in auto-partitioning,
  * we have essentially at-least-once delivery; in auto-partitioning with manual offsets, we
  * _possibly_ have at-least-once delivery; in manual-partitioning, we're on our own when it
  * comes to delivery semantics.
  */
class KafkaSubscriberActor extends Actor {
  var received: List[String] = List.empty

  // Set up automatically-supervised Kafka consumer whose life-cycle is
  // attached to this actor's life-cycle; however, it is not its child,
  // so it does not mess up its supervision strategies.
  //
  // The returned `Extractor` can be used in the `receive` PF to extract
  // the batch of records from Kafka.
  private val extractor = KafkaExtension(context.system)
    .actorConsumer
    .build(new StringDeserializer, new StringDeserializer)
    .autoPartition("test")

  override def receive: Receive = {
    case extractor(records) ⇒
      val values = records.recordsList.map(r ⇒ s"${r.key()}:${r.value()}")
      sender() ! KafkaConsumerActor.Confirm(records.offsets)
      received = received ++ values
  }

}
