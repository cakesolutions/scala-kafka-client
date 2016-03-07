package net.cakesolutions.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object KafkaConsumerActor {

  /**
    * Actor API - Initiate consumption from Kafka or reset an already started stream.
    *
    * @param offsets Consumption starts from specified offsets or kafka default, depending on (auto.offset.reset) setting.
    */
  case class Subscribe(offsets: Option[Offsets] = None)

  /**
    * Actor API - Confirm receipt of previous records.  If Offets are provided, they are committed synchronously to Kafka.
    * If no offsets provided, no commit is made.
    *
    * @param offsets Some(offsets) if a commit to Kafka is required.
    */
  case class Confirm(offsets: Option[Offsets] = None)

  /**
    * Actor API - Unsubscribe from Kafka.
    */
  case object Unsubscribe

  case class Offsets(offsetsMap: Map[TopicPartition, Long]) extends AnyVal {
    def get(topic: TopicPartition): Option[Long] = offsetsMap.get(topic)

    def forAllOffsets(that: Offsets)(f: (Long, Long) => Boolean): Boolean =
      offsetsMap.forall {
        case (topic, offset) => that.get(topic).forall(f(offset, _))
      }

    def toCommitMap: Map[TopicPartition, OffsetAndMetadata] =
      offsetsMap.mapValues(offset => new OffsetAndMetadata(offset))

    override def toString: String =
      offsetsMap
        .map { case (t, o) => s"$t: $o" }
        .mkString("Offsets(", ", ", ")")
  }

  case class Records[K: TypeTag, V: TypeTag](offsets: Offsets, records: ConsumerRecords[K, V]) {
    val keyTag = typeTag[K]
    val valueTag = typeTag[V]

    /**
      * Compare given types to record types.
      * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
      *
      * @tparam K1 the key type to compare to
      * @tparam V2 the value type to compare to
      * @return true when given types match objects type parameters, and false otherwise
      */
    def hasType[K1: TypeTag, V2: TypeTag]: Boolean =
      typeTag[K1].tpe <:< keyTag.tpe &&
        typeTag[V2].tpe <:< valueTag.tpe

    /**
      * Attempt to cast record keys and values to given types.
      * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
      *
      * @tparam K1 the key type to cast to
      * @tparam V2 the value type to cast to
      * @return the same records in casted form when casting is possible, and otherwise [[None]]
      */
    def cast[K1: TypeTag, V2: TypeTag]: Option[Records[K1, V2]] =
      if (hasType[K1, V2]) Some(this.asInstanceOf[Records[K1, V2]])
      else None

    /**
      * Get all the values as a single sequence.
      */
    def values: Seq[V] = records.toList.map(_.value())
  }

  object Conf {
    import scala.concurrent.duration.{MILLISECONDS => Millis}

    /**
      * Configuration for KafkaConsumerActor from Config
      *
      * @param config
      * @return
      */
    def apply(config: Config): Conf = {
      val topics = config.getStringList("consumer.topics")

      val scheduleInterval = Duration(config.getDuration("schedule.interval", Millis), Millis)
      val unconfirmedTimeout = Duration(config.getDuration("unconfirmed.timeout", Millis), Millis)

      apply(topics.toList, scheduleInterval, unconfirmedTimeout)
    }
  }

  /**
    * Configuration for KafkaConsumerActor
    *
    * @param topics             List of topics to subscribe to.
    * @param scheduleInterval   Poll Latency.
    * @param unconfirmedTimeout Seconds before unconfirmed messages is considered for redelivery.
    */
  case class Conf(topics: List[String],
                  scheduleInterval: FiniteDuration = 1000.millis,
                  unconfirmedTimeout: FiniteDuration = 3.seconds) {
    def withConf(config: Config): Conf = {
      this.copy(topics = config.getStringList("consumer.topics").toList)
    }
  }

  /**
    * KafkaConsumer config and the consumer actors config all contained in a Typesafe Config.
    */
  def props[K: TypeTag, V: TypeTag](conf: Config,
                                    keyDeserializer: Deserializer[K],
                                    valueDeserializer: Deserializer[V],
                                    nextActor: ActorRef): Props = {
    Props(
      new KafkaConsumerActor[K, V](KafkaConsumer.Conf[K, V](conf, keyDeserializer, valueDeserializer),
        KafkaConsumerActor.Conf(conf),
        nextActor))
  }

  /**
    * Construct with configured KafkaConsumer and Actor configurations.
    */
  def props[K: TypeTag, V: TypeTag](consumerConf: KafkaConsumer.Conf[K, V],
                                    actorConf: KafkaConsumerActor.Conf,
                                    nextActor: ActorRef): Props = {
    Props(new KafkaConsumerActor[K, V](consumerConf, actorConf, nextActor))
  }
}

class KafkaConsumerActor[K: TypeTag, V: TypeTag](consumerConf: KafkaConsumer.Conf[K, V], actorConf: KafkaConsumerActor.Conf, nextActor: ActorRef)
  extends Actor with ActorLogging with PollScheduling {

  import KafkaConsumerActor._
  import PollScheduling.Poll
  import context.become

  private val consumer = KafkaConsumer[K, V](consumerConf)
  private val trackPartitions = TrackPartitions(consumer)

  // Receive states
  private case class Unconfirmed(unconfirmed: Records[K, V], deliveryTime: LocalDateTime = LocalDateTime.now())
  private case class Buffered(unconfirmed: Records[K, V], deliveryTime: LocalDateTime = LocalDateTime.now(), buffered: Records[K, V])

  override def receive = unsubscribed

  private val unsubscribeReceive: Receive = {
    case Unsubscribe =>
      log.info("Unsubscribing")
      cancelPoll()
      consumer.unsubscribe()
      trackPartitions.clearOffsets()
      become(unsubscribed)
  }

  // Initial state
  def unsubscribed: Receive = {
    case Subscribe(offsets) =>
      log.info("Subscribing to topic(s): [{}]", actorConf.topics.mkString(", "))
      consumer.subscribe(actorConf.topics, trackPartitions)
      trackPartitions.seek(offsets.map(_.offsetsMap).getOrElse(Map.empty))
      log.info("To Ready state")
      become(ready)
      pollImmediate(200)
  }

  // No unconfirmed or buffered messages
  def ready: Receive = unsubscribeReceive orElse {
    case Poll(correlation, timeout) if isCurrentPoll(correlation) =>
      pollKafka(timeout) match {
        case Some(records) =>
          nextActor ! records
          log.info("To unconfirmed state")
          become(unconfirmed(Unconfirmed(records)))
          pollImmediate()

        case None =>
          schedulePoll()
      }
  }

  // Unconfirmed message with client, buffer empty
  def unconfirmed(state: Unconfirmed): Receive = unsubscribeReceive orElse {
    case Poll(correlation, _) if isCurrentPoll(correlation) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        nextActor ! state.unconfirmed
      }

      pollKafka() match {
        case Some(records) =>
          nextActor ! records
          log.info("To Buffer Full state")
          become(bufferFull(Buffered(state.unconfirmed, state.deliveryTime, records)))
          schedulePoll()

        case None =>
          schedulePoll()
      }

    case Confirm(offsetsO) =>
      log.info(s"Records confirmed")
      offsetsO.foreach { commitOffsets }
      log.info("To Ready state")
      become(ready)

      //immediate poll after confirm with block to reduce poll latency when backlog but processing is fast...
      pollImmediate(200)
  }

  //Buffered message and unconfirmed message
  def bufferFull(state: Buffered): Receive = unsubscribeReceive orElse {
    case Poll(correlation, _) if isCurrentPoll(correlation) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        nextActor ! state.unconfirmed
      }
      log.info(s"Buffer is full. Not gonna poll.")
      schedulePoll()

    case Confirm(offsetsO) =>
      log.info(s"Records confirmed")
      offsetsO.foreach { offsets => commitOffsets(offsets) }
      nextActor ! state.buffered
      log.info("To unconfirmed state")
      become(unconfirmed(Unconfirmed(state.buffered)))
      pollImmediate()
  }

  override def postStop(): Unit = {
    close()
  }

  /**
    * Attempt to get new records from Kafka
    *
    * @param timeout - specify a blocking poll timeout.  Default 0 for non blocking poll.
    * @return
    */
  private def pollKafka(timeout: Int = 0): Option[Records[K, V]] = {
    log.info("Poll Kafka for {} milliseconds", timeout)
    Try(consumer.poll(timeout)) match {
      case Success(rs) if rs.count() > 0 =>
        log.info("Records Received!")
        Some(Records(currentConsumerOffsets, rs))
      case Success(rs) =>
        None
      case Failure(_: WakeupException) =>
        log.warning("Poll was interrupted.")
        None
      case Failure(ex) =>
        log.error(ex, "Error occurred while attempting to poll Kafka!")
        None
    }
  }

  private def schedulePoll(): Unit = schedulePoll(actorConf.scheduleInterval)

  private def currentConsumerOffsets: Offsets = {
    val offsetsMap = consumer.assignment()
      .map(p => p -> consumer.position(p))
      .toMap
    Offsets(offsetsMap)
  }

  /**
    * True if records unconfirmed for longer than unconfirmedTimeoutSecs
    *
    * @return
    */
  private def isConfirmationTimeout(deliveryTime: LocalDateTime): Boolean = {
    deliveryTime.plus(actorConf.unconfirmedTimeout.toMillis, ChronoUnit.MILLIS) isBefore LocalDateTime.now()
  }

  private def commitOffsets(offsets: Offsets): Unit = {
    log.info("Committing offsets. {}", offsets)
    consumer.commitSync(offsets.toCommitMap)
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    if (!message.isInstanceOf[Poll])
      log.warning("Unknown message: {}", message)
  }

  private def close(): Unit = {
    consumer.close()
  }
}
