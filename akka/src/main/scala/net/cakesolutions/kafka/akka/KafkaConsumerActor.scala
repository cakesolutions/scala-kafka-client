package net.cakesolutions.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor.{Actor, ActorLogging, ActorRef}
import cakesolutions.kafka.KafkaConsumer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object KafkaConsumerActor {

  //Supported Actor messages
  sealed trait Command

  /**
   * Initiate consumption from Kafka or reset an already started stream.
   *
   * @param offsets Consumption starts from specified offsets or kafka default (TODO depending on which config)
   */
  case class Subscribe(offsets: Option[Offsets] = None) extends Command

  case class ConfirmOffsets(offsets: Offsets, commit: Boolean = false) extends Command

  case object Unsubscribe extends Command

  // Internal poll trigger
  private case object Poll extends Command

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

  case class Conf[K, V](conf: KafkaConsumer.Conf[K, V], topics: List[String])

  /**
   *
   * @param unconfirmedTimeoutSecs - Seconds before unconfirmed messages is considered for redelivery.
   * @param maxBuffer - Max number of record batches to cache.
   * @tparam K
   * @tparam V
   */
  private[akka] class ClientCache[K, V](unconfirmedTimeoutSecs: Long, maxBuffer: Int) {
    var unconfirmed: Option[Records[K, V]] = None
    var buffer = new mutable.Queue[Records[K, V]]()
    var deliveryTime: Option[LocalDateTime] = None

    def isFull: Boolean = buffer.size >= maxBuffer

    def bufferRecords(records: Records[K, V]) = buffer += records

    /**
     * If the client has no unconfirmed messages and there are messages in the buffer, get one to send and also add to 
     * unconfirmed.
     * @return
     */
    def getRecordsToDeliver(): Option[Records[K, V]] = {
      if (unconfirmed.isEmpty && buffer.nonEmpty) {
        val record = buffer.dequeue()
        unconfirmed = Some(record)
        deliveryTime = Some(LocalDateTime.now())
        Some(record)
      } else None
    }

    def getRedeliveryRecords: Records[K, V] = {
      assert(unconfirmed.isDefined)
      deliveryTime = Some(LocalDateTime.now())
      unconfirmed.get
    }

    def isMessageTimeout: Boolean = {
      deliveryTime match {
        case Some(time) =>
          time plus (unconfirmedTimeoutSecs, ChronoUnit.SECONDS) isBefore LocalDateTime.now()
        case None =>
          false
      }
    }

    def confirm(): Unit = {
      unconfirmed = None
    }

    def reset(): Unit = {
      unconfirmed = None
      buffer.clear()
      deliveryTime = None
    }
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

  private val consumer = KafkaConsumer[K, V](conf.conf)
  private val trackPartitions = TrackPartitions(consumer)

  // The actor's mutable state TODO params
  private val clientCache: ClientCache[K, V] = new ClientCache[K, V](1, 10)

  override def receive: Receive = {

    //Subscribe - start polling or reset offsets and begin again
    case Subscribe(offsets) =>
      log.info(s"Subscribing to topic(s): [${conf.topics.mkString(", ")}]")
      consumer.subscribe(conf.topics, trackPartitions)
      offsets.foreach(o => trackPartitions.offsets = o.offsetsMap)
      clientCache.reset()
      pollImmediate()

    //Confirm - no offsets needed? (commit mode is config defined)
    case ConfirmOffsets(offsets, commit) =>
      log.info(s"Confirm Offsets ${conf.topics}, $offsets")
      clientCache.confirm()
      clientCache.getRecordsToDeliver().foreach(records => sendRecords(records))

      //TODO
      if (commit) {
        commitOffsets(offsets)
      }

    //Internal
    case Poll =>
      log.info("poll")

      //Check for unconfirmed timed-out messages and redeliver
      if (clientCache.isMessageTimeout) {
        log.info("Message timed out, redelivering")
        sendRecords(clientCache.getRedeliveryRecords)
      }

      //Only poll kafka if buffer is not full
      if (clientCache.isFull) {
        log.info(s"Buffers are full. Not gonna poll. ${conf.topics}")
      } else {
        poll() foreach { records =>
          log.info("!Records")
          clientCache.bufferRecords(records)
        }
      }
      clientCache.getRecordsToDeliver().foreach(records => sendRecords(records))

    //TODO need to ignore future poll and stop scheduling poll and disconnect from Kafka.
    case Unsubscribe =>
      log.info("Unsubscribing")
      consumer.unsubscribe()
      clientCache.reset()

    //    case GracefulShutdown =>
    //      context.stop(self)
  }

  override def postStop(): Unit = {
    interrupt()
    close()
  }

  /**
   * Attempt to get new records from Kafka
   */
  private def poll(): Option[Records[K, V]] = {
    val result = Try(consumer.poll(0)) match {
      case Success(rs) if rs.count() > 0 =>
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

    if (result.isDefined) pollImmediate() else schedulePoll()

    result
  }

  private def interrupt(): Unit = {
    consumer.wakeup()
  }

  private def close(): Unit = {
    consumer.close()
  }

  //TODO remove
  private def sendRecords(records: Records[K, V]): Unit = {
    log.info("Delivering records")
    nextActor ! records
  }

  //TODO schedule conf
  private def schedulePoll(): Unit = {
    log.info("Schedule Poll")
    context.system.scheduler.scheduleOnce(3000 millis, self, Poll)(context.dispatcher)
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
    log.debug(s"Committing offsets. $offsets")
    consumer.commitSync(offsets.toCommitMap)
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }
}
