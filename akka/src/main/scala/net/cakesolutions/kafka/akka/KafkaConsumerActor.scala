package net.cakesolutions.kafka.akka

import akka.actor.{Actor, ActorLogging, ActorRef}
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

object KafkaConsumerActor {

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
        .map { case (t, o) => s"$t: $o"}
        .mkString("Offsets(", ", ", ")")
  }

  case class Records[K: TypeTag, V: TypeTag](offsets: Offsets, records: ConsumerRecords[K, V]) {
    val keyTag = typeTag[K]
    val valueTag = typeTag[V]

    def hasType[K1: TypeTag, V2: TypeTag]: Boolean =
      typeTag[K1].tpe <:< keyTag.tpe &&
        typeTag[V2].tpe <:< valueTag.tpe

    def cast[K1: TypeTag, V2: TypeTag]: Option[Records[K1, V2]] =
      if (hasType[K1, V2]) Some(this.asInstanceOf[Records[K1, V2]])
      else None

    def isNewerThan(that: Offsets): Boolean =
      offsets.forAllOffsets(that)(_ > _)

    def values: Seq[V] = records.toList.map(_.value())
  }

  sealed trait Command
  case class Reset(offsets: Offsets) extends Command
  case class ConfirmOffsets(offsets: Offsets, commit: Boolean = false) extends Command

  //Poll is private and internal only
  case object Poll extends Command

  case class Conf[K, V](consumerConfig: Config, topics: List[String])

  object State {
    def empty[K, V]: State[K, V] = State(None, None)
  }

  case class State[K, V](onTheFly: Option[Records[K, V]], buffer: Option[Records[K, V]]) {
    def offsetsAreOld(offsets: Offsets): Boolean =
      onTheFly.exists(_ isNewerThan offsets)

    def isFull: Boolean = onTheFly.nonEmpty && buffer.nonEmpty
  }

  val defaultConsumerConfig: Config =
    ConfigFactory.parseMap(Map(
      "enable.auto.commit" -> "false"
    ))
}

/**
 * @param conf
 * @param nextActor Consumed messages are pushed here.
 * @tparam K
 * @tparam V
 */
class KafkaConsumerActor[K: TypeTag, V: TypeTag](conf: KafkaConsumerActor.Conf[K, V], nextActor: ActorRef)
  extends Actor with ActorLogging {

  import KafkaConsumerActor._

  private val kafkaConfig = defaultConsumerConfig.withFallback(conf.consumerConfig)
  private val consumer = KafkaConsumer[K, V](kafkaConfig)
  private val trackPartitions = TrackPartitions(consumer)
//  private val counterConsume = KamonHelper.recorderOption(KafkaMetrics, self.path.name).map(_.pulledFromKafka)

  consumer.subscribe(conf.topics, trackPartitions)

  log.info(s"Starting consumer for topics [${conf.topics.mkString(", ")}]")

  private var state: State[K, V] = State.empty

  override def receive: Receive = {
    case Reset(offsets) =>
      log.info(s"Resetting offsets. ${conf.topics}, $offsets")
      trackPartitions.offsets = offsets.offsetsMap
      state = State.empty
      schedulePoll()

    case Poll if state.isFull =>
      log.info(s"Buffers are full. Not gonna poll. ${conf.topics}")

    case Poll =>
      log.info("poll")
      poll() foreach { records =>
        log.info("!Records")
        state = appendRecords(state, records)
      }

    case ConfirmOffsets(offsets, commit) =>
      log.info(s"Confirm Offsets ${conf.topics}, $offsets")
      log.warning("${state}")
      if (state.offsetsAreOld(offsets)) {
        log.info(s"Offsets old, resending ${conf.topics}")
        resendCurrentRecords(state)
      } else {
        log.info(s"Offsets new, ${conf.topics}")
        state = sendNextRecords(state)
        if (commit) {
          commitOffsets(offsets)
        }
      }

//    case GracefulShutdown =>
//      context.stop(self)
  }

  private def sendNextRecords(currentState: State[K, V]): State[K, V] = {
    currentState.buffer.orElse(poll()) match { // get the buffer or poll from Kafka
      case Some(records) => // Got records. Send them and try to fetch more.
        log.debug("Got next records. Sending.")
        sendRecords(records)
        currentState.copy(onTheFly = Some(records), buffer = poll())
      case None => // Got nothing, sending nothing. Poll has been scheduled.
        log.debug("No records available.")
        State.empty
    }
  }

  private def resendCurrentRecords(currentState: State[K, V]): Unit = {
    currentState.onTheFly.foreach { rs =>
      log.debug("Resending current records.")
      sendRecords(rs)
    }
  }

  //TODO change this
  private def appendRecords(currentState: State[K, V], records: Records[K, V]): State[K, V] = {
    (currentState.onTheFly, currentState.buffer) match {
      case (None, None) =>
        // Nothing stored. Send the records immediately.
        sendRecords(records)
        State(Some(records), None)
      case (Some(_), None) =>
        // Fill the buffer
        currentState.copy(buffer = Some(records))
      case _ =>
        // Polled too much. Ignoring the given records.
        currentState
    }
  }

  override def postStop(): Unit = {
    interrupt()
    close()
  }

  private def poll(): Option[Records[K, V]] = {
    val result = Try(consumer.poll(0)) match {
      case Success(rs) if rs.count() > 0 =>
//        counterConsume.foreach(_.increment(rs.count()))
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

    if (result.isEmpty) {
      // Always schedule a new poll when no results were received
      schedulePoll()
    } else {
      pollImmediate()
    }

    result
  }

  private def interrupt(): Unit = {
    consumer.wakeup()
  }

  private def close(): Unit = {
    consumer.close()
  }

  private def sendRecords(records: Records[K, V]): Unit = {
    nextActor ! records
  }

  //TODO schedule conf
  private def schedulePoll(): Unit = {
    log.info("Schedule Poll")
    context.system.scheduler.scheduleOnce(500 millis, self, Poll)(context.dispatcher)
  }

  private def pollImmediate(): Unit = {
    log.info("Poll immediate")
    self ! Poll
  }

  private def currentConsumerOffsets: Offsets = {
    val offsetsMap = consumer.assignment()
      .map(p => p -> consumer.position(p))
      .toMap
    Offsets(offsetsMap)
  }

  private def commitOffsets(offsets: Offsets): Unit = {
    log.debug(s"Committing offsets. ${offsets}")
    consumer.commitSync(offsets.toCommitMap)
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }
}
