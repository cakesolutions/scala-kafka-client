package cakesolutions.kafka.akka

import scala.reflect.runtime.universe.{TypeTag, typeOf, typeTag}

trait TypeTaggedTrait[Self] { self: Self =>

  /**
    * The representation of this class's type at a value level.
    *
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    */
  val selfTypeTag: TypeTag[Self]

  /**
    * Compare the given type to the type of this class
    *
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @return true if the types match and false otherwise
    */
  def hasType[Other: TypeTag]: Boolean =
    typeOf[Other] =:= selfTypeTag.tpe

  /**
    * Attempt to cast this value to the given type.
    *
    * If the types match, the casted version is returned. Otherwise, empty value is returned.
    *
    * Useful for regaining generic type information in runtime when it has been lost (e.g. in actor communication).
    *
    * @return casted version wrapped in `Some` if the cast succeeds, and otherwise `None`
    */
  def cast[Other: TypeTag]: Option[Other] =
    if (hasType[Other])
      Some(this.asInstanceOf[Other])
    else
      None
}

abstract class TypeTagged[Self: TypeTag] extends TypeTaggedTrait[Self] { self: Self =>
  val selfTypeTag: TypeTag[Self] = typeTag[Self]
}

/**
  * Extractor for [[TypeTaggedTrait]] instances.
  */
final class TypeTaggedExtractor[T: TypeTag] extends Extractor[Any, T] {
  override def unapply(a: Any): Option[T] = a match {
    case t: TypeTaggedTrait[_] => t.cast[T]
    case _ => None
  }
}