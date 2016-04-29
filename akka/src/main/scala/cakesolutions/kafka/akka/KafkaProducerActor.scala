package cakesolutions.kafka.akka

import akka.actor._
import cakesolutions.kafka.KafkaProducer
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success}

object KafkaProducerActor {
  import ProducerRecordMatcher.Matcher

  object Conf {
    def apply(config: Config): Conf = {
      val commit = config.getBoolean("producer.commit")
      apply(commit)
    }
  }

  case class Conf(commitToConsumer: Boolean)

  def props[K: TypeTag, V: TypeTag](producerConf: KafkaProducer.Conf[K, V], actorConf: Conf): Props = {
    val matcher = ProducerRecordMatcher.defaultMatcher[K, V](actorConf.commitToConsumer)
    propsWithMatcher(producerConf, actorConf, matcher)
  }

  def props[K: TypeTag, V: TypeTag](conf: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Props = {
    props(KafkaProducer.Conf(conf, keySerializer, valueSerializer), Conf(conf))
  }

  def propsWithMatcher[K, V](producerConf: KafkaProducer.Conf[K, V], actorConf: Conf, matcher: Matcher[K, V]): Props =
    Props(new KafkaProducerActor(producerConf, actorConf, matcher))

  def propsWithMatcher[K, V](conf: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V], matcher: Matcher[K, V]): Props = {
    propsWithMatcher(KafkaProducer.Conf(conf, keySerializer, valueSerializer), Conf(conf), matcher)
  }
}

protected class KafkaProducerActor[K, V](
  producerConf: KafkaProducer.Conf[K, V],
  actorConf: KafkaProducerActor.Conf,
  matcher: ProducerRecordMatcher.Matcher[K, V])
  extends Actor with ActorLogging {

  import context.dispatcher

  type Record = ProducerRecord[K, V]
  type Records = Iterable[Record]

  private val producer =
    try { KafkaProducer(producerConf) }
    catch { case ex: Exception => throw new KafkaProducerInitFail(cause = ex) }

  override def receive: Receive = matcher.andThen(handleResult)

  private def handleResult(result: ProducerRecordMatcher.Result[K, V]): Unit = {
    log.debug("Received a batch. Writing to Kafka.")
    val s = sender()
    sendMany(result.records).onComplete {
      case Success(_) =>
        log.debug("Wrote batch to Kafka.")
        result.response.foreach { response =>
          log.debug("Sending a response back.")
          s ! response
        }
      case Failure(err) =>
        logSendError(err)
    }
  }

  private def logSendError(err: Throwable): Unit =
    log.error(err, "Failed to send message to Kafka!")

  private def sendMany(records: Records) = Future.sequence(records.map(send))

  private def send(record: Record): Future[RecordMetadata] = {
    log.debug("Sending message to Kafka topic {} with key {}: {}", record.topic, record.key, record.value)
    producer.send(record)
  }

  override def postStop(): Unit = {
    log.info("KafkaProducerActor stopping")
    producer.close()
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning("Unknown message: {}", message)
  }
}

class KafkaProducerInitFail(
  message: String = "Error occurred while initializing Kafka producer!",
  cause: Throwable = null)
  extends Exception(message, cause)
