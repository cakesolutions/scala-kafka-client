package cakesolutions.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag

/**
  * An actor that wraps [[KafkaConsumer]].
  *
  * The actor pulls batches of messages from Kafka for all subscribed partitions,
  * and forwards them to the supplied Akka actor reference in [[ConsumerRecords]] format.
  *
  * Before the actor continues pulling more data from Kafka,
  * the receiver of the data must confirm the batches by sending back a [[KafkaConsumerActor.Confirm]] message
  * that contains the offsets from the received batch.
  * This mechanism allows the receiver to control the maximum rate of messages it will receive.
  * By including the received offsets in the confirmation message,
  * we avoid accidentally confirming batches that have not been fully processed yet.
  *
  * Actor's Kafka subscriptions can be controlled via the Actor Messages:
  * [[KafkaConsumerActor.Subscribe]] and [[KafkaConsumerActor.Unsubscribe]].
  *
  * For cases where there is Kafka or configuration issues, the Actor's supervisor strategy is applied.
  */
object KafkaConsumerActor {

  /**
    * Actor API - Initiate consumption from Kafka or reset an already started stream.
    * If Offsets are provided, the client is configured in 'Manually Assigned Partition mode' and the position is seeked
    * to the specified offset for each partition.  If no Offsets are provided the client is configured in 'Auto Partition mode'
    * and the position is set based on commit position and `auto.offset.reset` setting (usually 'EARLIEST' to avoid message loss).
    *
    * @param offsets Consumption starts from specified offsets in Manual assigned partition mode, if provided.  Auto Partition Mode otherwise.
    */
  case class Subscribe(offsets: Option[Offsets] = None)

  /**
    * Actor API - Confirm receipt of previous records.
    *
    * The message should provide the offsets that are to be confirmed.
    * If the offsets don't match the offsets that were last sent, the confirmation is ignored.
    * Offsets can be committed to Kafka using optional commit flag.
    *
    * @param offsets the offsets that are to be confirmed
    * @param commit  true to commit offsets
    */
  case class Confirm(offsets: Offsets, commit: Boolean = false)

  /**
    * Actor API - Unsubscribe from Kafka.
    */
  case object Unsubscribe

  protected[akka] case object Revoke

  case class ConsumerException(offsets: Option[Offsets] = None) extends Exception

  /**
    * Utilities for creating configurations for the [[KafkaConsumerActor]].
    */
  object Conf {

    import scala.concurrent.duration.{MILLISECONDS => Millis}

    /**
      * Create configuration for [[KafkaConsumerActor]] from Typesafe config.
      *
      * The given config must define the topics list. Other settings are optional overrides.
      * Expected configuration values:
      *
      * - topics: a list of topics to subscribe to
      * - schedule.interval: poll latency
      * - unconfirmed.timeout: Seconds before unconfirmed messages is considered for redelivery. To disable message redelivery provide a duration of 0.
      * - retryStrategy: Strategy to follow on Kafka driver failures. Default: infinitely on one second intervals
      */
    def apply(config: Config): Conf = {
      require(config.hasPath("topics"), "config must define topics")
      Conf(topics = Nil).withConf(config)
    }

    def durationFromConfig(config: Config, path: String) = Duration(config.getDuration(path, Millis), Millis)
  }

  /**
    * Configuration for [[KafkaConsumerActor]].
    *
    * @param topics             List of topics to subscribe to.
    * @param scheduleInterval   Poll Latency.
    * @param unconfirmedTimeout Seconds before unconfirmed messages is considered for redelivery.
    *                           To disable message redelivery provide a duration of 0.
    */
  case class Conf(topics: List[String],
                  scheduleInterval: FiniteDuration = 1000.millis,
                  unconfirmedTimeout: FiniteDuration = 3.seconds) {

    /**
      * Extend the config with additional Typesafe config.
      * The supplied config overrides existing properties.
      */
    def withConf(config: Config): Conf = {
      this.copy(
        topics = if (config.hasPath("topics")) config.getStringList("topics").toList else topics,
        scheduleInterval = if (config.hasPath("schedule.interval")) Conf.durationFromConfig(config, "schedule.interval") else scheduleInterval,
        unconfirmedTimeout = if (config.hasPath("unconfirmed.timeout")) Conf.durationFromConfig(config, "unconfirmed.timeout") else unconfirmedTimeout
      )
    }
  }

  /**
    * Create Akka `Props` for [[KafkaConsumerActor]] from a Typesafe config.
    *
    * @param conf              Typesafe config containing all the [[KafkaConsumer.Conf]] and [[KafkaConsumerActor.Conf]] related configurations.
    * @param keyDeserializer   deserializer for the key
    * @param valueDeserializer deserializer for the value
    * @param downstreamActor   the actor where all the consumed messages will be sent to
    * @tparam K key deserialiser type
    * @tparam V value deserialiser type
    */
  def props[K: TypeTag, V: TypeTag](conf: Config,
                                    keyDeserializer: Deserializer[K],
                                    valueDeserializer: Deserializer[V],
                                    downstreamActor: ActorRef): Props = {
    props(
      KafkaConsumer.Conf[K, V](conf, keyDeserializer, valueDeserializer),
      KafkaConsumerActor.Conf(conf),
      downstreamActor
    )
  }

  /**
    * Create Akka `Props` for [[KafkaConsumerActor]].
    *
    * @param consumerConf    configurations for the [[KafkaConsumer]]
    * @param actorConf       configurations for the [[KafkaConsumerActor]]
    * @param downstreamActor the actor where all the consumed messages will be sent to
    * @tparam K key deserialiser type
    * @tparam V value deserialiser type
    */
  def props[K: TypeTag, V: TypeTag](consumerConf: KafkaConsumer.Conf[K, V],
                                    actorConf: KafkaConsumerActor.Conf,
                                    downstreamActor: ActorRef): Props = {
    Props(new KafkaConsumerActor[K, V](consumerConf, actorConf, downstreamActor))
  }
}

private class KafkaConsumerActor[K: TypeTag, V: TypeTag](
                                                          consumerConf: KafkaConsumer.Conf[K, V],
                                                          actorConf: KafkaConsumerActor.Conf,
                                                          downstreamActor: ActorRef)
  extends Actor with ActorLogging with PollScheduling {

  import KafkaConsumerActor._
  import PollScheduling.Poll
  import context.become

  type Records = ConsumerRecords[K, V]

  private val consumer = KafkaConsumer[K, V](consumerConf)

  // Handles partition reassignments in the KafkaClient
  private val trackPartitions = TrackPartitions(consumer, context.self)
  private val isTimeoutUsed = actorConf.unconfirmedTimeout.toMillis > 0
  private val delayedPollTimeout = 200

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
      log.info("Unsubscribing from Kafka")
      cancelPoll()
      unsubscribe()
      become(unsubscribed)

      //
    case Revoke =>
      log.info("Revoking Assignments!")
      become(ready)
    case Subscribe(_) =>
      log.warning("Attempted to subscribe while consumer was already subscribed")

    case poll: Poll if !isCurrentPoll(poll) =>
    // Do nothing
  }

  private def unsubscribe(): Unit = {
    consumer.unsubscribe()
    trackPartitions.clearOffsets()
  }

  // Initial state
  def unsubscribed: Receive = {
    case Unsubscribe =>
      log.info("Already unsubscribed")

    case Subscribe(offsets) =>
      log.info("Subscription received for topic(s): [{}]", actorConf.topics.mkString(", "))
      subscribe(offsets)
      log.debug("To Ready state")
      become(ready)
      pollImmediate(delayedPollTimeout)

    // TODO check:::
    case Confirm(offsets, commit) =>
      log.info("Subscribing to topic(s): [{}]", actorConf.topics.mkString(", "))
      subscribe(Some(offsets))
      if (commit) commitOffsets(offsets)
      log.debug("To Ready state")
      become(ready)
      pollImmediate(delayedPollTimeout)

    case _: Poll =>
    // Do nothing

  }

  // No unconfirmed or buffered messages
  def ready: Receive = unsubscribeReceive orElse {
    case Poll(correlation, timeout) if isCurrentPoll(correlation) =>
      pollKafka(timeout) {
        case Some(records) =>
          sendRecords(records)
          log.debug("To unconfirmed state")
          become(unconfirmed(Unconfirmed(records)))
          pollImmediate()

        case None =>
          schedulePoll()
      }
  }

  // Unconfirmed message with client, buffer empty
  def unconfirmed(state: Unconfirmed): Receive = unconfirmedCommonReceive(state) orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("Records timed-out awaiting confirmation, redelivering")
        sendRecords(state.unconfirmed)
        become(unconfirmed(Unconfirmed(state.unconfirmed, LocalDateTime.now())))
      }

      // If the last commit caused a partition revocation, we don't poll to allow the unconfirmed to flush through, prior to
      // the rebalance completion.
      if (trackPartitions.isRevoked) {
        log.info("Not polling as revoked!!!")
      } else {
        pollKafka() {
          case Some(records) =>
            log.debug("To Buffer Full state")
            become(bufferFull(Buffered(state.unconfirmed, state.deliveryTime, records)))
            schedulePoll()

          case None =>
            schedulePoll()
        }
      }

    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      if (commit) commitOffsets(offsets)
      log.debug("To Ready state")
      become(ready)

      // Immediate poll after confirm with block to reduce poll latency in case the is a backlog in Kafka but processing is fast.
      pollImmediate(delayedPollTimeout)
  }

  // Buffered message and unconfirmed message with the client.  No need to poll until its confirmed, or timed out.
  def bufferFull(state: Buffered): Receive = unconfirmedCommonReceive(state) orElse {
    case poll: Poll if isCurrentPoll(poll) =>

      // If an confirmation timeout is set and has expired, the message is redelivered
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("Records timed-out awaiting confirmation, redelivering")
        sendRecords(state.unconfirmed)
        become(bufferFull(Buffered(state.unconfirmed, LocalDateTime.now(), state.buffered)))
      }
      log.debug(s"Buffer is full. Not gonna poll.")
      schedulePoll()

    // The next message can be sent immediately from the buffer.  A poll to Kafka for new messages for the buffer also happens immediately.
    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      if (commit) commitOffsets(offsets)
      sendRecords(state.buffered)
      log.debug("To unconfirmed state")
      become(unconfirmed(Unconfirmed(state.buffered)))
      pollImmediate()
  }

  private def subscribe(offsetsO: Option[Offsets]): Unit = offsetsO match {
    case Some(offsets) =>
      log.info("Subscribing, in manual partition assignment mode to Partitions/Offsets:" + offsets.toString)
      consumer.assign(offsets.topicPartitions.toList)
      offsets.offsetsMap.foreach {
        case (key, value) =>
          log.info(s"Seek to $key, $value")
          consumer.seek(key, value)
      }
    case None =>
      log.info(s"Subscribing, in auto partition assignment mode to Topics: ${actorConf.topics} with group.id: ${consumerConf.props("group.id")}")
      consumer.subscribe(actorConf.topics, trackPartitions)
  }

  private def sendRecords(records: Records): Unit = {
    downstreamActor ! records
  }

  // The client is usually misusing the Consumer if incorrect Confirm offsets are provided
  private def unconfirmedCommonReceive(state: HasUnconfirmedRecords): Receive = unsubscribeReceive orElse {
    case Confirm(offsets, _) if !state.isCurrentOffset(offsets) =>
      log.warning("Received confirmation for unexpected offsets: {}", offsets)
  }

  override def postStop(): Unit = {
    log.info("KafkaConsumerActor stopping")
    close()
  }

  /**
    * Attempt to get new records from Kafka,
    *
    * @param timeout - specify a blocking poll timeout.  Default 0 for non blocking poll.
    * @return
    */
  private def pollKafka(timeout: Int = 0)(cb: Option[Records] => Unit): Unit =
    tryWithConsumer(currentConsumerOffsets) {
      log.debug("Poll Kafka for {} milliseconds", timeout)
      val rs = consumer.poll(timeout)
      log.debug("Poll Complete!")
      if (rs.count() > 0)
        cb(Some(ConsumerRecords(currentConsumerOffsets, rs)))
      else
        cb(None)
    }

  private def tryWithConsumer(offsets: Offsets)(effect: => Unit): Unit = {
    try {
      effect
    } catch {
      case we: WakeupException =>
        log.warning("Wakeup Exception, ignoring", we)
      case cfe: CommitFailedException =>
        log.warning("Exception while commiting, ignoring: " + cfe.getMessage)
      case error: Exception =>
        log.info("EXCEPTION")
        //TODO evaluate parameter here
        throw ConsumerException(Some(offsets))
    }
  }

  private def schedulePoll(): Unit = schedulePoll(actorConf.scheduleInterval)

  private def currentConsumerOffsets: Offsets = {
    val offsetsMap = consumer.assignment()
      .map(p => p -> consumer.position(p))
      .toMap
    Offsets(offsetsMap)
  }

  @scala.throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)

    reason match {
      case ConsumerException(offsetsO) =>
        log.warning(s"KafkaConsumerActor restart.  Resubscribing with Offsets: $offsetsO", reason)
        self ! Subscribe(offsetsO)
      case _ =>
        throw new RuntimeException("Unexpected exception thrown by KafkaConsumerActor", reason)
    }
  }

  /**
    * True if records unconfirmed for longer than unconfirmedTimeoutSecs.
    */
  private def isConfirmationTimeout(deliveryTime: LocalDateTime): Boolean =
    isTimeoutUsed && timeoutTime(deliveryTime).isBefore(LocalDateTime.now())

  private def timeoutTime(deliveryTime: LocalDateTime) =
    deliveryTime.plus(actorConf.unconfirmedTimeout.toMillis, ChronoUnit.MILLIS)

  private def commitOffsets(offsets: Offsets): Unit = {
    log.debug("Committing offsets. {}", offsets)

    val currentOffsets = currentConsumerOffsets
    val currentPartitions = currentOffsets.topicPartitions
    val offsetsToCommit = offsets.keepOnly(currentPartitions)
    val nonCommittedOffsets = offsets.remove(currentPartitions)

    if (nonCommittedOffsets.nonEmpty) {
      val tps = nonCommittedOffsets.topicPartitions.mkString(", ")
      log.warning(s"Cannot commit offsets for partitions the consumer is not subscribed to: $tps")
    }

    tryWithConsumer(currentOffsets) {
      consumer.commitSync(offsetsToCommit.toCommitMap)
    }
    log.debug("Committing complete")
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }

  private def close(): Unit = try {
    consumer.close()
  } catch {
    case ex: Exception => log.error(ex, "Error occurred while closing consumer")
  }
}
