package cakesolutions.kafka.akka

import akka.actor._
import cakesolutions.kafka.KafkaProducer
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.Future
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success}

/**
  * An actor that wraps [[KafkaProducer]].
  *
  * The actor takes incoming (batches of) Kafka records, writes them to Kafka,
  * and responds to sender once the messages have been written.
  *
  * [[KafkaProducerActor]] is not tied to any specific topic,
  * but it's message serializers have to be specified before it's used.
  *
  * The types of messages that [[KafkaProducerActor]] consumes is controlled by a [[KafkaProducerActor.Matcher]].
  * By default, the actor accepts all [[ProducerRecords]] messages which have key and value types
  * matching the producer actor's type parameters.
  */
object KafkaProducerActor {

  /**
    * Kafka writable records received from [[Matcher]].
    *
    * @param records the records that are to be written to Kafka
    * @param response optional message that is to be sent back to the sender after messages have been written to Kafka
    * @tparam K Kafka message key type
    * @tparam V Kafka message value type
    */
  case class MatcherResult[K, V](records: Iterable[ProducerRecord[K, V]], response: Option[Any])

  /**
    * A partial function that extracts producer records from messages sent to [[KafkaProducerActor]].
    *
    * @tparam K Kafka message key type
    * @tparam V Kafka message value type
    */
  type Matcher[K, V] = PartialFunction[Any, MatcherResult[K, V]]

  /**
    * The default [[Matcher]] that is used for extracting Kafka records from incoming messages.
    * Accepts all [[ProducerRecords]] messages which have matching key and value types.
    *
    * @tparam K Kafka message key type
    * @tparam V Kafka message value type
    */
  def defaultMatcher[K: TypeTag, V: TypeTag]: Matcher[K, V] = {
    val extractor = ProducerRecords.extractor[K, V]

    {
      case extractor(ingestible) => MatcherResult(ingestible.records, ingestible.response)
    }
  }

  /**
    * Create Akka `Props` for [[KafkaProducerActor]].
    *
    * @param producerConf configurations for the [[KafkaProducer]]
    * @tparam K key serializer type
    * @tparam V valu serializer type
    */
  def props[K: TypeTag, V: TypeTag](producerConf: KafkaProducer.Conf[K, V]): Props = {
    val matcher = defaultMatcher[K, V]
    propsWithMatcher(producerConf, matcher)
  }

  /**
    * Create Akka `Props` for [[KafkaProducerActor]] from a Typesafe config.
    *
    * @param conf configurations for the [[KafkaProducer]]
    * @param keySerializer serializer for the key
    * @param valueSerializer serializer for the value
    * @tparam K key serializer type
    * @tparam V value serializer type
    */
  def props[K: TypeTag, V: TypeTag](conf: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V]): Props = {
    props(KafkaProducer.Conf(conf, keySerializer, valueSerializer))
  }

  /**
    * Create Akka `Props` for [[KafkaProducerActor]] with a custom [[Matcher]].
    *
    * @param producerConf configurations for the [[KafkaProducer]]
    * @param matcher custom matcher for mapping incoming messages to Kafka writable messages
    * @tparam K key serializer type
    * @tparam V value serializer type
    */
  def propsWithMatcher[K, V](producerConf: KafkaProducer.Conf[K, V], matcher: Matcher[K, V]): Props =
    Props(new KafkaProducerActor(producerConf, matcher))

  /**
    * Create Akka `Props` for [[KafkaProducerActor]] from a Typesafe config with a custom [[Matcher]].
    *
    * @param conf configurations for the [[KafkaProducer]]
    * @param keySerializer serializer for the key
    * @param valueSerializer serializer for the value
    * @param matcher custom matcher for mapping incoming messages to Kafka writable messages
    * @tparam K key serializer type
    * @tparam V value serializer type
    */
  def propsWithMatcher[K, V](conf: Config, keySerializer: Serializer[K], valueSerializer: Serializer[V], matcher: Matcher[K, V]): Props = {
    propsWithMatcher(KafkaProducer.Conf(conf, keySerializer, valueSerializer), matcher)
  }
}

private class KafkaProducerActor[K, V](
  producerConf: KafkaProducer.Conf[K, V],
  matcher: KafkaProducerActor.Matcher[K, V])
  extends Actor with ActorLogging {

  import context.dispatcher
  import KafkaProducerActor.MatcherResult

  type Record = ProducerRecord[K, V]
  type Records = Iterable[Record]

  private val producer =
    try { KafkaProducer(producerConf) }
    catch { case ex: Exception => throw new KafkaProducerInitFail(cause = ex) }

  override def receive: Receive = matcher.andThen(handleResult)

  private def handleResult(result: MatcherResult[K, V]): Unit = {
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
