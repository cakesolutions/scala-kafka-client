package cakesolutions.kafka.akka

import org.apache.kafka.clients.producer.ProducerRecord
import scala.reflect.runtime.universe.TypeTag

object ProducerRecordMatcher {

  case class Result[K, V](records: Iterable[ProducerRecord[K, V]], response: Option[Any])

  type Matcher[K, V] = PartialFunction[Any, Result[K, V]]

  def defaultMatcher[K: TypeTag, V: TypeTag](commitToConsumer: Boolean): Matcher[K, V] = {
    def convertResponse(any: Any): Any = any match {
      case offsets: Offsets => KafkaConsumerActor.Confirm(offsets, commit = commitToConsumer)
      case a => a
    }

    val extractor = KafkaIngestible.extractor[K, V]

    {
      case extractor(ingestible) => Result(ingestible.records, ingestible.response.map(convertResponse))
    }
  }
}
