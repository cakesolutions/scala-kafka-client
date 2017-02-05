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

import scala.collection.JavaConverters._
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
  final case class Confirm(offsets: Offsets, commit: Boolean = false) extends MessageApi

  /**
    * Sent when the actor is backing off delivery of messages due to suspected backpressure
    * caused by missed deliveries.
    * @param redeliveryCount the current redelivery count
    */
  final case class BackingOff(redeliveryCount: Int) extends MessageApi

  /**
    * Actor API - Initiate consumption from Kafka or reset an already started stream.
    *
    * Subscription has four modes which provide a combination of either auto or manual partition assignment and either Kafka
    * managed or self managed commit offsets.
    *
    *  - Auto partition: Kafka manages partition assignments between members of a consumer group.  Offset commit points for each
    *    partition are managed by Kafka.
    *
    *  - Auto partition with manual offset: Kafka manages partition assignments between members of a consumer group.  Offset commit
    *    points are maintained by the client.
    *
    *  - Manual partition: Topic and partitions are specified by the client.  Offset commit points for each
    *    partition are managed by Kafka.
    *
    *  - Manual offset: Topic and partitions are specified by the client.  Offset commit
    *    points are maintained by the client.
    */
  sealed trait Subscribe extends MessageApi

  object Subscribe {

    /**
      * Subscribe to topics in auto assigned partition mode, relying on Kafka to manage the commit point for each partition.
      * This is this simplest and most common subscription mode that provides a parallel streaming capability with at-least-once
      * semantics.
      *
      * In auto assigned partition mode, the consumer partitions are managed by Kafka.
      * This means that they can get automatically rebalanced with other consumers consuming from the same topic with the same group-id.
      *
      * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
      *
      * The client should ensure that received records are confirmed with 'commit = true' to ensure kafka tracks the commit point.
      *
      * @param topics the topics to subscribe to start consuming from
      */
    final case class AutoPartition(topics: Iterable[String]) extends Subscribe

    /**
      * Subscribe to topics in auto assigned partition mode with client managed offset commit positions for each partition.
      * This subscription mode is typically used when performing some parallel stateful computation and storing the offset
      * position along with the state in some kind of persistent store.  This allows for exactly-once state manipulation against
      * an at-least-once delivery stream.
      *
      * The client should provide callbacks to receive notifications of when partitions have been assigned or revoked.  When
      * a partition has been assigned, the client should lookup the latest offsets for the given partitions from its store, and supply
      * those.  The KafkaConsumerActor will seek to the specified positions.
      *
      * The client should ensure that received records are confirmed with 'commit = false' to ensure consumed records are
      * not committed back to kafka.
      *
      * @param topics the topics to subscribe to start consuming from
      * @param assignedListener a callback handler that should lookup the latest offsets for the provided topic/partitions.
      * @param revokedListener a callback to provide the oppurtunity to cleanup any in memory state for revoked partitions.
      */
    final case class AutoPartitionWithManualOffset(
      topics: Iterable[String],
      assignedListener: List[TopicPartition] => Offsets,
      revokedListener: List[TopicPartition] => Unit
    ) extends Subscribe

    /**
      * Subscribe to topics in manually assigned partition mode, relying on Kafka to manage the commit point for each partition.
      *
      * In manually assigned partition mode, the consumer will specify the partitions directly, this means that Kafka will not be automatically
      * rebalance the partitions when new consumers appear in the consumer group.
      *
      * The message consumption starting point will be decided by offset reset strategy in the consumer configuration.
      *
      * The client should ensure that received records are confirmed with 'commit = true' to ensure kafka tracks the commit point.
      *
      * @param topicPartitions the topics with partitions to start consuming from
      */
    final case class ManualPartition(topicPartitions: Iterable[TopicPartition]) extends Subscribe

    /**
      * Subscribe to topics in manually assigned partition mode, with client managed offset commit positions for each partition.
      *
      * In manually assigned partition mode, the consumer will specify the partitions directly,
      * This means that Kafka will not be automatically rebalance the partitions when new consumers appear in the consumer group.
      *
      * In addition to manually assigning the partitions, the partition offsets will be set to start from the given offsets.
      *
      * The client should ensure that received records are confirmed with 'commit = false' to ensure consumed records are
      * not committed back to kafka.
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

  /**
    * Internal message indicating partitions have been reassigned.
    */
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
    * @param maxRedeliveries    Maximum number of times an unconfirmed message will be redelivered downstream.
    *                           Redeliveries are only attempted if unconfirmedTimeout > 0.
    */
  final case class Conf(
    scheduleInterval: FiniteDuration = 1000.millis,
    unconfirmedTimeout: FiniteDuration = 3.seconds,
    maxRedeliveries: Int = 3
  ) {

    /**
      * Extend the config with additional Typesafe config.
      * The supplied config overrides existing properties.
      */
    def withConf(config: Config): Conf =
      copy(
        scheduleInterval = if (config.hasPath("schedule.interval")) Conf.durationFromConfig(config, "schedule.interval") else scheduleInterval,
        unconfirmedTimeout = if (config.hasPath("unconfirmed.timeout")) Conf.durationFromConfig(config, "unconfirmed.timeout") else unconfirmedTimeout,
        maxRedeliveries= if (config.hasPath("max.redeliveries")) config.getInt("max.redeliveries") else maxRedeliveries
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
  def props[K: TypeTag, V: TypeTag](
    conf: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    downstreamActor: ActorRef
  ): Props =
    props(
      KafkaConsumer.Conf[K, V](conf, keyDeserializer, valueDeserializer),
      KafkaConsumerActor.Conf(conf),
      downstreamActor
    )

  /**
    * Create Akka `Props` for [[KafkaConsumerActor]].
    *
    * @param consumerConf    configurations for the [[KafkaConsumer]]
    * @param actorConf       configurations for the [[KafkaConsumerActor]]
    * @param downstreamActor the actor where all the consumed messages will be sent to
    * @tparam K key deserialiser type
    * @tparam V value deserialiser type
    */
  def props[K: TypeTag, V: TypeTag](
    consumerConf: KafkaConsumer.Conf[K, V],
    actorConf: KafkaConsumerActor.Conf,
    downstreamActor: ActorRef
  ): Props =
    Props(new KafkaConsumerActorImpl[K, V](consumerConf, actorConf, downstreamActor))

  /**
    * Create a [[KafkaConsumerActor]] from a Typesafe config.
    *
    * @param conf Typesafe config containing all the [[KafkaConsumer.Conf]] and [[KafkaConsumerActor.Conf]] related configurations.
    * @param keyDeserializer deserializer for the key
    * @param valueDeserializer deserializer for the value
    * @param downstreamActor the actor where all the consumed messages will be sent to
    * @tparam K key deserialiser type
    * @tparam V value deserialiser type
    * @param actorFactory the actor factory to create the actor with
    */
  def apply[K: TypeTag, V: TypeTag](
    conf: Config,
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V],
    downstreamActor: ActorRef
  )(implicit actorFactory: ActorRefFactory): KafkaConsumerActor = {
    val p = props(conf, keyDeserializer, valueDeserializer, downstreamActor)
    val ref = actorFactory.actorOf(p)
    fromActorRef(ref)
  }

  /**
    * Create a [[KafkaConsumerActor]].
    *
    * @param consumerConf configurations for the [[KafkaConsumer]]
    * @param actorConf configurations for the [[KafkaConsumerActor]]
    * @param downstreamActor the actor where all the consumed messages will be sent to
    * @tparam K key deserialiser type
    * @tparam V value deserialiser type
    * @param actorFactory the actor factory to create the actor with
    */
  def apply[K: TypeTag, V: TypeTag](
    consumerConf: KafkaConsumer.Conf[K, V],
    actorConf: KafkaConsumerActor.Conf,
    downstreamActor: ActorRef
  )(implicit actorFactory: ActorRefFactory): KafkaConsumerActor = {
    val p = props(consumerConf, actorConf, downstreamActor)
    val ref = actorFactory.actorOf(p)
    fromActorRef(ref)
  }

  /**
    * Create a [[KafkaConsumerActor]] wrapper from an existing ActorRef.
    */
  def fromActorRef(ref: ActorRef): KafkaConsumerActor = new KafkaConsumerActor(ref)
}

/**
  * Classic, non-Akka API for interacting with [[KafkaConsumerActor]].
  */
final class KafkaConsumerActor private (val ref: ActorRef) {
  import KafkaConsumerActor.{Confirm, Subscribe, Unsubscribe}

  /**
    * Initiate consumption from Kafka or reset an already started stream.
    *
    * @param subscription Either an AutoPartition, ManualPartition or ManualOffsets subscription.
    */
  def subscribe(subscription: Subscribe): Unit = ref ! subscription

  /**
    * Unsubscribe from Kafka
    */
  def unsubscribe(): Unit = ref ! Unsubscribe

  /**
    * Confirm receipt of previous records.
    *
    * The message should provide the offsets that are to be confirmed.
    * If the offsets don't match the offsets that were last sent, the confirmation is ignored.
    * Offsets can be committed to Kafka using optional commit flag.
    *
    * @param offsets the offsets that are to be confirmed
    * @param commit  true to commit offsets
    */
  def confirm(offsets: Offsets, commit: Boolean = false): Unit = ref ! Confirm(offsets, commit)
}

private final class KafkaConsumerActorImpl[K: TypeTag, V: TypeTag](
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
  private var trackPartitions:TrackPartitions = new EmptyTrackPartitions

  private val isTimeoutUsed = actorConf.unconfirmedTimeout.toMillis > 0
  private val delayedPollTimeout = 200

  // Receive states
  private sealed trait StateData {
    val subscription: Subscribe
    val lastConfirmedOffsets: Option[Offsets]

    def scheduleInterval: FiniteDuration = actorConf.scheduleInterval

    def toSubscribed: Subscribed = Subscribed(subscription, lastConfirmedOffsets)

    def advanceSubscription: Subscribe = {
      def advance(offsets: Offsets) = subscription match {
        case s: Subscribe.AutoPartition => s
        case s: Subscribe.AutoPartitionWithManualOffset =>
          Subscribe.AutoPartitionWithManualOffset(s.topics, s.assignedListener, s.revokedListener)
        case _: Subscribe.ManualPartition => Subscribe.ManualOffset(offsets)
        case _: Subscribe.ManualOffset => Subscribe.ManualOffset(offsets)
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

    /**
      * Number of attempts that have been made to deliver the unconfirmed records downstream
      */
    def redeliveryCount: Int

    def noBackoffNeeded(): Boolean = redeliveryCount < actorConf.maxRedeliveries

    /**
      * Naive strategy to increment poll backoff when in redelivery.
      * @return
      */
    override def scheduleInterval: FiniteDuration = redeliveryCount * super.scheduleInterval + super.scheduleInterval

    def isCurrentOffset(offsets: Offsets): Boolean = unconfirmed.offsets == offsets
  }

  private final case class Unconfirmed(
     subscription: Subscribe,
     lastConfirmedOffsets: Option[Offsets],
     unconfirmed: Records,
     deliveryTime: LocalDateTime = LocalDateTime.now(),
     redeliveryCount: Int = 0
  ) extends UnconfirmedRecordsStateData {

    def confirm(offsets: Offsets): Subscribed = Subscribed(subscription, Some(offsets))

    def redelivered: Unconfirmed =
      copy(deliveryTime = LocalDateTime.now(), redeliveryCount = redeliveryCount + 1)

    def addToBuffer(buffered: Records): Buffered =
      Buffered(subscription, lastConfirmedOffsets, unconfirmed, deliveryTime, buffered, redeliveryCount)
  }

  private final case class Buffered(
    subscription: Subscribe,
    lastConfirmedOffsets: Option[Offsets],
    unconfirmed: Records,
    deliveryTime: LocalDateTime = LocalDateTime.now(),
    buffered: Records,
    redeliveryCount: Int = 0
  ) extends UnconfirmedRecordsStateData {

    def confirm(offsets: Offsets): Unconfirmed =
      Unconfirmed(subscription, Some(offsets), buffered, LocalDateTime.now())

    def redelivered: Buffered =
      copy(deliveryTime = LocalDateTime.now(), redeliveryCount = redeliveryCount + 1)
  }

  override def receive: Receive = unsubscribed

  // Initial state
  private val unsubscribed: Receive = terminatedDownstreamReceive orElse {
    case Unsubscribe =>
      log.info("Already unsubscribed")

    case sub: Subscribe =>
      subscribe(sub)
      log.debug("To Ready state")
      become(ready(Subscribed(sub, None)))
      pollImmediate(delayedPollTimeout)

    case Confirm(_, _) =>
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
  private def ready(state: Subscribed): Receive = subscribedCommonReceive(state) orElse terminatedDownstreamReceive orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      pollKafka(state, poll.timeout) match {
        case Some(records) =>
          sendRecords(records)
          log.debug("To unconfirmed state")
          become(unconfirmed(state.toUnconfirmed(records)))
          pollImmediate()

        case None =>
          schedulePoll(stateData = state)
      }

    case c: Confirm =>
      log.info("Received a confirmation while nothing was unconfirmed. Offsets: {}", c.offsets)
  }

  // Unconfirmed message with client, buffer empty
  private def unconfirmed(state: Unconfirmed): Receive = unconfirmedCommonReceive(state) orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("In unconfirmed: records timed out while waiting for a confirmation.")
        if (state.noBackoffNeeded()) {
          log.info("In unconfirmed: redelivering.")
          sendRecords(state.unconfirmed)
        } else {
          log.info("In unconfirmed: backing off.")
          downstreamActor ! BackingOff(state.redeliveryCount)
        }
        become(unconfirmed(state.redelivered))
      }

      // If the last commit caused a partition revocation,
      // we don't poll to allow the unconfirmed to flush through, prior to the rebalance completion.
      if (trackPartitions.isRevoked) {
        log.info("Partitions revoked. Not polling.")
        schedulePoll(stateData = state)
      } else {
        pollKafka(state, poll.timeout) match {
          case Some(records) =>
            log.debug("To Buffer Full state")
            become(bufferFull(state.addToBuffer(records)))
            schedulePoll(stateData = state)
          case None =>
            schedulePoll(stateData = state)
        }
      }

    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      val updatedState = state.confirm(offsets)

      val commitResult = if (commit) commitOffsets(updatedState, offsets) else Success({})
      commitResult match {
        case Success(_) =>
          log.debug("To Ready state")
          become(ready(updatedState))

          // Immediate poll after confirm with block to reduce poll latency in case the is a backlog in Kafka but processing is fast.
          pollImmediate(delayedPollTimeout)
        case Failure(_) =>
          log.debug("To RevokeAwait State")
          become(revokeAwait(updatedState, offsets))
          schedulePoll(stateData = state)
      }
  }

  // Buffered message and unconfirmed message with the client.  No need to poll until its confirmed, or timed out.
  private def bufferFull(state: Buffered): Receive = unconfirmedCommonReceive(state) orElse terminatedDownstreamReceive orElse {
    case poll: Poll if isCurrentPoll(poll) =>
      // If an confirmation timeout is set and has expired, the message is redelivered
      if (isConfirmationTimeout(state.deliveryTime)) {
        log.info("In bufferFull: records timed out while waiting for a confirmation.")
        if (state.noBackoffNeeded()) {
          log.info("In bufferFull: redelivering.")
          sendRecords(state.unconfirmed)
        } else {
          log.info("In bufferFull: backing off.")
          downstreamActor ! BackingOff(state.redeliveryCount)
        }
        become(bufferFull(state.redelivered))
      }
      log.debug(s"Buffer is full. Not going to poll.")
      schedulePoll(stateData = state)

    // The next message can be sent immediately from the buffer.  A poll to Kafka for new messages for the buffer also happens immediately.
    case Confirm(offsets, commit) if state.isCurrentOffset(offsets) =>
      log.debug("Records confirmed")
      val updatedState = state.confirm(offsets)

      val commitResult =
        if (commit) commitOffsets(updatedState, offsets)
        else Success({})

      commitResult match {
        case Success(_) =>
          sendRecords(updatedState.unconfirmed)
          log.debug("To unconfirmed state")
          become(unconfirmed(updatedState))
          pollImmediate()
        case Failure(_) =>
          log.debug("To RevokeAwait State")
          become(revokeAwait(updatedState, offsets))
          schedulePoll(stateData = state)
      }
  }

  /**
    * A state after a commit failure, awaiting confirmation of a rebalance to occur.  We can either continue processing
    * if the rebalance completes and no existing partition assignments are removed, otherwise we clear down state are resume
    * from last committed offsets, which may result in some unavoidable redelivery.
    * @param offsets The offsets of the last delivered records that failed to commit to Kafka
    */
  private def revokeAwait(state: StateData, offsets: Offsets): Receive = terminatedDownstreamReceive  orElse {
    case RevokeResume =>
      log.info("RevokeResume - Resuming processing post rebalance")
      state match {
        case u: Unconfirmed =>
          sendRecords(u.unconfirmed)
          become(unconfirmed(u))
        case b: Buffered =>
          sendRecords(b.unconfirmed)
          become(bufferFull(b))
        case s: Subscribed =>
          become(ready(s))
      }

    case RevokeReset =>
      log.warning("RevokeReset - Resetting state to Committed offsets")
      become(ready(Subscribed(state.subscription, None)))

    case poll: Poll if isCurrentPoll(poll) =>
      log.debug("Poll in Revoke")
      pollKafka(state, poll.timeout) match {
        case Some(records) =>
          state match {
            case s: Subscribed =>
              become(revokeAwait(s.toUnconfirmed(records), offsets))
            case u: Unconfirmed =>
              become(revokeAwait(u.addToBuffer(records), offsets))
            case b: Buffered =>
              throw consumerFailure(b)
          }
          schedulePoll(stateData = state)

        case None =>
          schedulePoll(stateData = state)
    }

    case c: Confirm =>
      log.info("Received a confirmation while waiting for rebalance to finish. Received offsets: {}", c.offsets)
  }

  private def subscribe(s: Subscribe): Unit = s match {
    case Subscribe.AutoPartition(topics) =>
      log.info(s"Subscribing in auto partition assignment mode to topics [{}].", topics.mkString(","))
      trackPartitions = new TrackPartitionsCommitMode(consumer, context.self)
      consumer.subscribe(topics.toList.asJava, trackPartitions)

    case Subscribe.AutoPartitionWithManualOffset(topics, assignedListener, revokedListener) =>
      log.info(s"Subscribing in auto partition assignment with manual offset mode to topics [{}].", topics.mkString(","))
      trackPartitions = new TrackPartitionsManualOffset(consumer, context.self, assignedListener, revokedListener)
      consumer.subscribe(topics.toList.asJava, trackPartitions)

    case Subscribe.ManualPartition(topicPartitions) =>
      log.info("Subscribing in manual partition assignment mode to topic/partitions [{}].", topicPartitions.mkString(","))
      consumer.assign(topicPartitions.toList.asJava)

    case Subscribe.ManualOffset(offsets) =>
      log.info("Subscribing in manual partition assignment mode to partitions with offsets [{}]", offsets)
      consumer.assign(offsets.topicPartitions.toList.asJava)
      seekOffsets(offsets)
  }

  // The client is usually misusing the Consumer if incorrect Confirm offsets are provided
  private def unconfirmedCommonReceive(state: UnconfirmedRecordsStateData): Receive =
    subscribedCommonReceive(state) orElse {
      case Confirm(offsets, _) if !state.isCurrentOffset(offsets) =>
        log.warning("Received confirmation for unexpected offsets: {}", offsets)
    }

  private def terminatedDownstreamReceive: Receive = {
    case Terminated(`downstreamActor`) =>
      log.info("Downstream Actor terminated")
      context stop self
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
  private def pollKafka(state: StateData, timeout: Int): Option[Records] =
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
        log.info("Wakeup Exception, ignoring.")
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
      consumer.commitSync(offsetsToCommit.toCommitMap.asJava)
      Success({})
    } catch {
      case we: WakeupException =>
        log.debug("Wakeup Exception. Ignoring.")
        Success({})
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

  private def schedulePoll(stateData: StateData): Unit = schedulePoll(stateData.scheduleInterval)

  private def currentConsumerOffsets: Offsets = {
    val offsetsMap = consumer.assignment().asScala
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

  override def preStart(): Unit = {
    context.watch(downstreamActor)
  }

  private def close(): Unit = try {
    consumer.close()
  } catch {
    case ex: Exception => log.error(ex, "Error occurred while closing consumer")
  }
}
