package cakesolutions.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.Config
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
    * Actor API - Confirm receipt of previous records.
    * The message should provide the offsets that are to be confirmed.
    * If the offsets don't match the offsets that were last sent, the confirmation is ignored.
    * Offsets can be committed to Kafka using optional [[commit]] flag.
    *
    * @param offsets the offsets that are to be confirmed
    * @param commit true to commit offsets
    */
  case class Confirm(offsets: Offsets, commit: Boolean = false)

  /**
    * Actor API - Unsubscribe from Kafka.
    */
  case object Unsubscribe

  object Conf {
    import scala.concurrent.duration.{MILLISECONDS => Millis}

    /**
      * Configuration for KafkaConsumerActor from Config
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
    * @param unconfirmedTimeout Seconds before unconfirmed messages is considered for redelivery.  To disable message redelivery
    *                           provide a duration of 0.
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
    props(
      KafkaConsumer.Conf[K, V](conf, keyDeserializer, valueDeserializer),
        KafkaConsumerActor.Conf(conf),
        nextActor)
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

  type Records = KeyValuesWithOffsets[K, V]

  private val consumer = KafkaConsumer[K, V](consumerConf)
  private val trackPartitions = TrackPartitions(consumer)
  private val isTimeoutUsed = actorConf.unconfirmedTimeout.toMillis > 0

  // Receive states
  private sealed trait HasUnconfirmedRecords {
    val unconfirmed: Records
    def isCurrentOffset(offsets: Offsets): Boolean = unconfirmed.offsets == offsets
  }

  private case class Unconfirmed(unconfirmed: Records, deliveryTime: LocalDateTime = LocalDateTime.now())
    extends HasUnconfirmedRecords

  private case class Buffered(unconfirmed: Records, deliveryTime: LocalDateTime = LocalDateTime.now(), buffered: Records)
    extends HasUnconfirmedRecords

  override def receive = unsubscribed

  private val unsubscribeReceive: Receive = {
    case Unsubscribe =>
      log.info("Unsubscribing")
      cancelPoll()
      consumer.unsubscribe()
      trackPartitions.clearOffsets()
      become(unsubscribed)

    case Subscribe(_) =>
      log.warning("Attempted to subscribe while consumer was already subscribed")

    case Poll(correlationId, _) if !isCurrentPoll(correlationId) =>
      // Do nothing
  }

  // Initial state
  def unsubscribed: Receive = {
    case Unsubscribe =>
      log.info("Already unsubscribed")

    case Subscribe(offsets) =>
      log.info("Subscribing to topic(s): [{}]", actorConf.topics.mkString(", "))
      trackPartitions.seek(offsets.map(_.offsetsMap).getOrElse(Map.empty))
      consumer.subscribe(actorConf.topics, trackPartitions)
      log.debug("To Ready state")
      become(ready)
      pollImmediate(200)

    case Poll(correlationId, _) if !isCurrentPoll(correlationId) =>
      // Do nothing
  }

  // No unconfirmed or buffered messages
  def ready: Receive = unsubscribeReceive orElse {
    case Poll(correlation, timeout) if isCurrentPoll(correlation) =>
      pollKafka(timeout) match {
        case Some(records) =>
          nextActor ! records
          log.debug("To unconfirmed state")
          become(unconfirmed(Unconfirmed(records)))
          pollImmediate()

        case None =>
          schedulePoll()
      }
  }

  // Unconfirmed message with client, buffer empty
  def unconfirmed(state: Unconfirmed): Receive = unconfirmedCommonReceive(state) orElse {
    case Poll(correlation, _) if isCurrentPoll(correlation) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        nextActor ! state.unconfirmed
      }

      pollKafka() match {
        case Some(records) =>
          nextActor ! records
          log.debug("To Buffer Full state")
          become(bufferFull(Buffered(state.unconfirmed, state.deliveryTime, records)))
          schedulePoll()

        case None =>
          schedulePoll()
      }

    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.info(s"Records confirmed")
      if (commit) commitOffsets(offsets)
      log.debug("To Ready state")
      become(ready)

      // Immediate poll after confirm with block to reduce poll latency in case the is a backlog in Kafka but processing is fast.
      pollImmediate(200)
  }

  // Buffered message and unconfirmed message
  def bufferFull(state: Buffered): Receive = unconfirmedCommonReceive(state) orElse {
    case Poll(correlation, _) if isCurrentPoll(correlation) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        nextActor ! state.unconfirmed
      }
      log.debug(s"Buffer is full. Not gonna poll.")
      schedulePoll()

    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.info(s"Records confirmed")
      if (commit) commitOffsets(offsets)
      nextActor ! state.buffered
      log.debug("To unconfirmed state")
      become(unconfirmed(Unconfirmed(state.buffered)))
      pollImmediate()
  }

  private def unconfirmedCommonReceive(state: HasUnconfirmedRecords): Receive = unsubscribeReceive orElse {
    case Confirm(offsets, _) if !state.isCurrentOffset(offsets) =>
      log.info("Received confirmation for unexpected offsets: {}", offsets)
  }

  override def postStop(): Unit = {
    close()
  }

  /**
    * Attempt to get new records from Kafka,
    *
    * @param timeout - specify a blocking poll timeout.  Default 0 for non blocking poll.
    * @return
    */
  private def pollKafka(timeout: Int = 0): Option[Records] = {
    log.debug("Poll Kafka for {} milliseconds", timeout)
    Try(consumer.poll(timeout)) match {
      case Success(rs) if rs.count() > 0 =>
        log.debug("Records Received!")
        Some(KeyValuesWithOffsets(currentConsumerOffsets, rs))
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
    * True if records unconfirmed for longer than unconfirmedTimeoutSecs.
    */
  private def isConfirmationTimeout(deliveryTime: LocalDateTime): Boolean =
    isTimeoutUsed && timeoutTime(deliveryTime).isBefore(LocalDateTime.now())

  private def timeoutTime(deliveryTime: LocalDateTime) =
    deliveryTime.plus(actorConf.unconfirmedTimeout.toMillis, ChronoUnit.MILLIS)

  private def commitOffsets(offsets: Offsets): Unit = {
    log.info("Committing offsets. {}", offsets)
    consumer.commitSync(offsets.toCommitMap)
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }

  private def close(): Unit = {
    consumer.close()
  }
}
