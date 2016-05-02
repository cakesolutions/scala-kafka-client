package cakesolutions.kafka.akka

import org.apache.kafka.clients.producer.ProducerRecord
import scala.reflect.runtime.universe.TypeTag

object ProducerRecordMatcher {

  case class Result[K, V](records: Iterable[ProducerRecord[K, V]], response: Option[Any])

  type Matcher[K, V] = PartialFunction[Any, Result[K, V]]

  def defaultMatcher[K: TypeTag, V: TypeTag]: Matcher[K, V] = {
    val extractor = KafkaIngestible.extractor[K, V]

    {
      case extractor(ingestible) => Result(ingestible.records, ingestible.response)
    }
  }
}
