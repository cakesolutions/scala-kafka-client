package cakesolutions.kafka.akka

/**
  * Scala's extractor pattern (unapply) encapsulated in a trait.
  *
  * @see [[http://danielwestheide.com/blog/2012/11/21/the-neophytes-guide-to-scala-part-1-extractors.html Neophyte's Guide to Scala on extractors]]
  * @tparam I the input value to extract a value from
  * @tparam O the value that is to be extracted
  */
trait Extractor[I, O] {
  def unapply(input: I): Option[O]
}

/**
  * Helper functions for [[Extractor]].
  */
object Extractor {
  private final class Ext[I, O](f: I => Option[O]) extends Extractor[I, O] {
    override def unapply(input: I): Option[O] = f(input)
  }

  /**
    * Create an [[Extractor]] from a function.
    *
    * @param f extractor function
    * @tparam I type of the input to extract from
    * @tparam O type of the extracted value
    * @return an extractor
    */
  def apply[I, O](f: I => Option[O]): Extractor[I, O] = new Ext(f)
}
