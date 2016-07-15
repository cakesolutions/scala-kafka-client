package cakesolutions.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor._
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

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

  private[akka] sealed trait FullMessageApi

  /**
    * Actor API
    */
  sealed trait MessageApi extends FullMessageApi

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
  case class Confirm(offsets: Offsets, commit: Boolean = false) extends MessageApi

  /**
    * Actor API - Initiate consumption from Kafka or reset an already started stream.
    *
    * Subscription has three modes:
    *
    *  - Auto partition: provide the topics to subscribe to, and let Kafka manage partition delegation between consumers.
    *  - Manual partition: provide the topics with partitions, and let Kafka decide the point where to begin consuming messages from.
    *  - Manual offset: provide the topics with partitions and offsets for precise control.
    */
  sealed trait Subscribe extends MessageApi

  object Subscribe {

    /**
      * Subscribe to topics in auto assigned partition mode.
      *
      * In auto assigned partition mode, the consumer partitions are managed by Kafka.
      * This means that they can get automatically rebalanced with other consumers consuming from the same topic.
      *
      * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
      *
      * @param topics the topics to subscribe to start consuming from
      */
    final case class AutoPartition(topics: Iterable[String]) extends Subscribe

    /**
      * Subscribe to topics in manually assigned partition mode.
      *
      * In manually assigned partition mode, the consumer partitions are managed by the client.
      * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
      *
      * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
      *
      * @param topicPartitions the topics with partitions to start consuming from
      */
    final case class ManualPartition(topicPartitions: Iterable[TopicPartition]) extends Subscribe

    /**
      * Subscribe to topics in manually assigned partition mode, and set the offsets to consume from.
      *
      * In manually assigned partition mode, the consumer partitions are managed by the client.
      * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
      *
      * In addition to manually assigning the partitions, the partition offsets will be set to start from the given offsets.
      *
      * @param offsets the topics with partitions and offsets to start consuming from
      */
    final case class ManualOffset(offsets: Offsets) extends Subscribe
  }

  /**
    * Actor API - Unsubscribe from Kafka.
    */
  case object Unsubscribe extends MessageApi

  private[akka] sealed trait InternalMessageApi extends FullMessageApi

  /**
    * Internal message indicating partitions have been revoked.
    */
  private[akka] case object RevokeReset extends InternalMessageApi

  private[akka] case object RevokeResume extends InternalMessageApi

  /**
    * Internal message used for triggering a [[ConsumerException]]
    */
  private[akka] case object TriggerConsumerFailure extends InternalMessageApi

  /**
    * Exception type escalated through supervision to indicate an unrecoverable error.
    *
    * The last known subscription is included as part of the exception to support resubscription attempt on actor restart.
    */
  final case class ConsumerException(
    lastSubscription: Option[Subscribe],
    message: String = "Exception thrown from Kafka consumer!",
    cause: Throwable = null
  ) extends Exception(message, cause)

  final case class KafkaConsumerInitFail(
    message: String = "Error occurred while initializing Kafka consumer!",
    cause: Throwable = null
  ) extends Exception(message, cause)

  /**
    * Utilities for creating configurations for the [[KafkaConsumerActor]].
    */
  object Conf {

    import scala.concurrent.duration.{MILLISECONDS => Millis}

    /**
      * Create configuration for [[KafkaConsumerActor]] from Typesafe config.
      *
      * Expected configuration values:
      *
      *  - schedule.interval: poll latency (default 1 second)
      *  - unconfirmed.timeout: Seconds before unconfirmed messages is considered for redelivery. To disable message redelivery provide a duration of 0. (default 3 seconds)
      */
    def apply(config: Config): Conf = Conf().withConf(config)

    def durationFromConfig(config: Config, path: String) = Duration(config.getDuration(path, Millis), Millis)
  }

  /**
    * Configuration for [[KafkaConsumerActor]].
    *
    * @param scheduleInterval   Poll Latency.
    * @param unconfirmedTimeout Seconds before unconfirmed messages is considered for redelivery.
    *                           To disable message redelivery provide a duration of 0.
    */
  case class Conf(
    scheduleInterval: FiniteDuration = 1000.millis,
    unconfirmedTimeout: FiniteDuration = 3.seconds
  ) {

    /**
      * Extend the config with additional Typesafe config.
      * The supplied config overrides existing properties.
      */
    def withConf(config: Config): Conf =
      copy(
        scheduleInterval = if (config.hasPath("schedule.interval")) Conf.durationFromConfig(config, "schedule.interval") else scheduleInterval,
        unconfirmedTimeout = if (config.hasPath("unconfirmed.timeout")) Conf.durationFromConfig(config, "unconfirmed.timeout") else unconfirmedTimeout
      )
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
  downstreamActor: ActorRef
) extends Actor with ActorLogging with PollScheduling {

  import KafkaConsumerActor._
  import PollScheduling.Poll
  import context.become

  type Records = ConsumerRecords[K, V]

  private val consumer = KafkaConsumer[K, V](consumerConf)

  // Handles partition reassignments in the kafka client
  private val trackPartitions = TrackPartitions(consumer, context.self)
  private val isTimeoutUsed = actorConf.unconfirmedTimeout.toMillis > 0
  private val delayedPollTimeout = 200

  // Receive states
  private sealed trait StateData {
    val subscription: Subscribe
    val lastConfirmedOffsets: Option[Offsets]

    def toSubscribed: Subscribed = Subscribed(subscription, lastConfirmedOffsets)

    def advanceSubscription: Subscribe = {
      def advance(offsets: Offsets) = subscription match {
        case _: Subscribe.ManualOffset => Subscribe.ManualOffset(offsets)
        case _: Subscribe.ManualPartition => Subscribe.ManualOffset(offsets)
        case s: Subscribe.AutoPartition => s
      }
      lastConfirmedOffsets.map(advance).getOrElse(subscription)
    }
  }

  private final case class Subscribed(
    subscription: Subscribe,
    lastConfirmedOffsets: Option[Offsets]
  ) extends StateData {

    def toUnconfirmed(unconfirmed: Records): Unconfirmed = Unconfirmed(subscription, lastConfirmedOffsets, unconfirmed)
  }

  private sealed trait UnconfirmedRecordsStateData extends StateData {
    val unconfirmed: Records

    def isCurrentOffset(offsets: Offsets): Boolean = unconfirmed.offsets == offsets
  }

  private final case class Unconfirmed(
    subscription: Subscribe,
    lastConfirmedOffsets: Option[Offsets],
    unconfirmed: Records,
    deliveryTime: LocalDateTime = LocalDateTime.now()
  ) extends UnconfirmedRecordsStateData {

    def confirm(offsets: Offsets): Subscribed = Subscribed(subscription, Some(offsets))

    def redelivered: Unconfirmed =
      copy(deliveryTime = LocalDateTime.now())

    def addToBuffer(buffered: Records): Buffered =
      Buffered(subscription, lastConfirmedOffsets, unconfirmed, deliveryTime, buffered)
  }

  private final case class Buffered(
    subscription: Subscribe,
    lastConfirmedOffsets: Option[Offsets],
    unconfirmed: Records,
    deliveryTime: LocalDateTime = LocalDateTime.now(),
    buffered: Records
  ) extends UnconfirmedRecordsStateData {

    def confirm(offsets: Offsets): Unconfirmed =
      Unconfirmed(subscription, Some(offsets), buffered, LocalDateTime.now())

    def redelivered: Buffered =
      copy(deliveryTime = LocalDateTime.now())
  }

  override def receive = unsubscribed

  // Initial state
  private def unsubscribed: Receive = {
    case Unsubscribe =>
      log.info("Already unsubscribed")

    case sub: Subscribe =>
      subscribe(sub)
      log.debug("To Ready state")
      become(ready(Subscribed(sub, None)))
      pollImmediate(delayedPollTimeout)

    case Confirm(offsets, commit) =>
      log.warning("Attempted to confirm offsets while consumer wasn't subscribed")

    case _: Poll => // Do nothing
  }

  private def subscribedCommonReceive(state: StateData): Receive = {
    case Unsubscribe =>
      log.info("Unsubscribing from Kafka")
      cancelPoll()
      unsubscribe()
      become(unsubscribed)

    case RevokeReset =>
      log.info("Revoking Assignments - resetting state!")
      become(ready(state.toSubscribed))

    case _: Subscribe =>
      log.warning("Attempted to subscribe while consumer was already subscribed")

    case TriggerConsumerFailure =>
      log.info("Triggering consumer failure!")
      throw consumerFailure(state)

    case RevokeResume => //Do nothing

    case poll: Poll if !isCurrentPoll(poll) => // Do nothing
  }

  private def unsubscribe(): Unit = {
    consumer.unsubscribe()
    trackPartitions.reset()
  }

  // No unconfirmed or buffered messages
  private def ready(state: Subscribed): Receive = subscribedCommonReceive(state) orElse {
    case Poll(correlation, timeout) if isCurrentPoll(correlation) =>
      pollKafka(state) match {
        case Some(records) =>
          sendRecords(records)
          log.debug("To unconfirmed state")
          become(unconfirmed(state.toUnconfirmed(records)))
          pollImmediate()

        case None =>
          schedulePoll()
      }
  }

  // Unconfirmed message with client, buffer empty
  private def unconfirmed(state: Unconfirmed): Receive = unconfirmedCommonReceive(state) orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("Records timed-out awaiting confirmation, redelivering")
        sendRecords(state.unconfirmed)
        become(unconfirmed(state.redelivered))
      }

      // If the last commit caused a partition revocation,
      // we don't poll to allow the unconfirmed to flush through, prior to the rebalance completion.
      if (trackPartitions.isRevoked) {
        log.info("Partitions revoked. Not polling.")
        schedulePoll()
      } else {
        pollKafka(state) match {
          case Some(records) =>
            log.debug("To Buffer Full state")
            become(bufferFull(state.addToBuffer(records)))
            schedulePoll()
          case None =>
            schedulePoll()
        }
      }

    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      val updatedState = state.confirm(offsets)

      val commitResult = if (commit) commitOffsets(updatedState, offsets) else Success()
      commitResult match {
        case Success(_) =>
          log.debug("To Ready state")
          become(ready(updatedState))

          // Immediate poll after confirm with block to reduce poll latency in case the is a backlog in Kafka but processing is fast.
          pollImmediate(delayedPollTimeout)
        case Failure(_) =>
          log.debug("To RevokeAwait State")
          become(revokeAwait(updatedState, offsets))
          schedulePoll()
      }
  }

  // Buffered message and unconfirmed message with the client.  No need to poll until its confirmed, or timed out.
  private def bufferFull(state: Buffered): Receive = unconfirmedCommonReceive(state) orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      // If an confirmation timeout is set and has expired, the message is redelivered
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("Records timed-out awaiting confirmation, redelivering")
        sendRecords(state.unconfirmed)
        become(bufferFull(state.redelivered))
      }
      log.debug(s"Buffer is full. Not gonna poll.")
      schedulePoll()

    // The next message can be sent immediately from the buffer.  A poll to Kafka for new messages for the buffer also happens immediately.
    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      val updatedState = state.confirm(offsets)

      val commitResult =
        if (commit) commitOffsets(updatedState, offsets)
        else Success()

      commitResult match {
        case Success(_) =>
          sendRecords(updatedState.unconfirmed)
          log.debug("To unconfirmed state")
          become(unconfirmed(updatedState))
          pollImmediate()
        case Failure(_) =>
          log.debug("To RevokeAwait State")
          become(revokeAwait(updatedState, offsets))
          schedulePoll()
      }
  }

  /**
    * A state after a commit failure, awaiting confirmation of a rebalance to occur.  We can either continue processing
    * if the rebalance completes and no existing partition assignments are removed, otherwise we clear down state are resume
    * from last committed offsets, which may result in some unavoidable redelivery.
    * @param state
    * @param offsets The offsets of the last delivered records that failed to commit to Kafka
    */
  private def revokeAwait(state: StateData, offsets: Offsets): Receive = {
    case RevokeResume =>
      log.info("RevokeResume - resuming processing post rebalance")
      state match {
        case u: Unconfirmed =>
          sendRecords(u.unconfirmed)
          become(ready(u.confirm(offsets)))
        case b: Buffered =>
          sendRecords(b.unconfirmed)
          become(unconfirmed(b.confirm(offsets)))
      }

    case RevokeReset =>
      log.warning("RevokeReset - Resetting state to Committed offsets")
      become(ready(Subscribed(state.subscription, None)))

    case poll: Poll if isCurrentPoll(poll) =>
      log.info("Poll in Revoke")
      pollKafka(state) match {
        case Some(records) =>
          state match {
            case s: Subscribed =>
              become(revokeAwait(s.toUnconfirmed(records), offsets))
            case u: Unconfirmed =>
              become(revokeAwait(u.addToBuffer(records), offsets))
          }
          schedulePoll()

        case None =>
          schedulePoll()
    }
  }

  private def subscribe(s: Subscribe): Unit = s match {
    case Subscribe.AutoPartition(topics) =>
      log.info(s"Subscribing in auto partition assignment mode to topics [{}].", topics.mkString(","))
      consumer.subscribe(topics.toList, trackPartitions)

    case Subscribe.ManualPartition(topicPartitions) =>
      log.info("Subscribing in manual partition assignment mode to topic/partitions [{}].", topicPartitions.mkString(","))
      consumer.assign(topicPartitions.toList)

    case Subscribe.ManualOffset(offsets) =>
      log.info("Subscribing in manual partition assignment mode to partitions with offsets [{}]", offsets)
      consumer.assign(offsets.topicPartitions.toList)
      seekOffsets(offsets)
  }

  // The client is usually misusing the Consumer if incorrect Confirm offsets are provided
  private def unconfirmedCommonReceive(state: UnconfirmedRecordsStateData): Receive =
    subscribedCommonReceive(state) orElse {
      case Confirm(offsets, _) if !state.isCurrentOffset(offsets) =>
        log.warning("Received confirmation for unexpected offsets: {}", offsets)
    }

  private def seekOffsets(offsets: Offsets): Unit =
    offsets.offsetsMap.foreach {
      case (key, value) =>
        log.info(s"Seek to $key, $value")
        consumer.seek(key, value)
    }

  private def sendRecords(records: Records): Unit = {
    downstreamActor ! records
  }

  /**
    * Attempt to get new records from Kafka,
    *
    * @param timeout - specify a blocking poll timeout.  Default 0 for non blocking poll.
    */
  private def pollKafka(state: StateData, timeout: Int = 0): Option[Records] =
    tryWithConsumer(state) {
      log.debug("Poll Kafka for {} milliseconds", timeout)
      val rs = consumer.poll(timeout)
      log.debug("Poll Complete!")
      if (rs.count() > 0)
        Some(ConsumerRecords(currentConsumerOffsets, rs))
      else
        None
    }

  private def tryWithConsumer[T](state: StateData)(effect: => Option[T]): Option[T] = {
    try {
      effect
    } catch {
      case we: WakeupException =>
        log.debug("Wakeup Exception. Ignoring.")
        None
      case error: Exception =>
        log.info("Exception thrown from Kafka Consumer")
        throw consumerFailure(state, error)
    }
  }

  private def commitOffsets(state: StateData, offsets: Offsets): Try[Unit] = {
    log.debug("Committing offsets. {}", offsets)

    val currentOffsets = currentConsumerOffsets
    val currentPartitions = currentOffsets.topicPartitions
    val offsetsToCommit = offsets.keepOnly(currentPartitions)
    val nonCommittedOffsets = offsets.remove(currentPartitions)

    if (nonCommittedOffsets.nonEmpty) {
      log.warning(s"Cannot commit offsets for partitions the consumer is not subscribed to: {}",
        nonCommittedOffsets.topicPartitions.mkString(", "))
    }

    tryCommit(offsetsToCommit, state)
  }

  private def tryCommit(offsetsToCommit: Offsets, state: StateData): Try[Unit] = {
    try {
      consumer.commitSync(offsetsToCommit.toCommitMap)
      Success()
    } catch {
      case we: WakeupException =>
        log.debug("Wakeup Exception. Ignoring.")
        Success()
      case cfe: CommitFailedException =>
        log.warning("Exception while committing {}", cfe.getMessage)
        Failure(cfe)
      case error: Exception =>
        log.info("Exception thrown from Kafka Consumer")
        throw consumerFailure(state, error)
    }
  }

  private def consumerFailure(state: StateData, cause: Exception = null) =
    ConsumerException(Some(state.advanceSubscription), cause = cause)

  private def schedulePoll(): Unit = schedulePoll(actorConf.scheduleInterval)

  private def currentConsumerOffsets: Offsets = {
    val offsetsMap = consumer.assignment()
      .map(p => p -> consumer.position(p))
      .toMap
    Offsets(offsetsMap)
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    recoverFromException(reason)
  }

  private def recoverFromException(ex: Throwable): Unit = ex match {
    case ConsumerException(lastSubscription, message, _) =>
      log.warning(s"KafkaConsumerActor restarted: {}", message)
      lastSubscription.foreach { sub =>
        log.info("Resubscribing: {}", sub)
        self ! sub
      }
    case _ =>
      throw new RuntimeException("Unexpected exception thrown by KafkaConsumerActor", ex)
  }

  /**
    * True if records unconfirmed for longer than unconfirmedTimeoutSecs.
    */
  private def isConfirmationTimeout(deliveryTime: LocalDateTime): Boolean =
    isTimeoutUsed && timeoutTime(deliveryTime).isBefore(LocalDateTime.now())

  private def timeoutTime(deliveryTime: LocalDateTime) =
    deliveryTime.plus(actorConf.unconfirmedTimeout.toMillis, ChronoUnit.MILLIS)

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }

  override def postStop(): Unit = {
    log.info("KafkaConsumerActor stopping")
    close()
  }

  private def close(): Unit = try {
    consumer.close()
  } catch {
    case ex: Exception => log.error(ex, "Error occurred while closing consumer")
  }
}
