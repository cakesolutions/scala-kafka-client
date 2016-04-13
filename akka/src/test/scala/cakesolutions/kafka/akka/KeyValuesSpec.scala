package cakesolutions.kafka.akka

import org.scalatest.{FlatSpecLike, Inside, Matchers}

class KeyValuesSpec extends FlatSpecLike with Matchers with Inside {

  val knownInput: KeyValues[String, Int] = KeyValues(Some("foo"), Seq(1, 2, 3))
  val partiallyKnownInput: KeyValues[_, _] = knownInput
  val anyInput: Any = knownInput

  "KeyValues" should "match types correctly" in {
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
    val correctExt = KeyValues.extractor[String, Int]
    val incorrectExt = KeyValues.extractor[Int, String]

    anyInput should not matchPattern {
      case incorrectExt(_) =>
    }

    inside(anyInput) {
      case correctExt(kvs) =>
        kvs shouldEqual knownInput
    }
  }
}
