package cakesolutions.kafka.akka

import org.scalatest.{FlatSpecLike, Inside, Matchers}

class ConsumerRecordsSpec extends FlatSpecLike with Matchers with Inside {

  val partition = ("sometopic", 0)
  val knownInput: ConsumerRecords[String, Int] = ConsumerRecords.fromPairs(partition, Seq(Some("foo") -> 1))
  val partiallyKnownInput: ConsumerRecords[_, _] = knownInput
  val anyInput: Any = knownInput

  "ConsumerRecords" should "match types correctly" in {
    partiallyKnownInput.hasType[String, Int] shouldEqual true
    partiallyKnownInput.hasType[Int, String] shouldEqual false
  }

  it should "cast to only correct types" in {
    val success = partiallyKnownInput.cast[String, Int]
    val failure = partiallyKnownInput.cast[Int, String]

    success shouldEqual Some(knownInput)
    failure shouldEqual None
  }

  it should "extract values correctly" in {
    val correctExt = ConsumerRecords.extractor[String, Int]
    val incorrectExt = ConsumerRecords.extractor[Int, String]

    anyInput should not matchPattern {
      case incorrectExt(_) =>
    }

    inside(anyInput) {
      case correctExt(kvs) =>
        kvs shouldEqual knownInput
    }
  }
}
