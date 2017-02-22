package cakesolutions.kafka

import java.util.Properties
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import scala.collection.immutable._
import scala.collection.mutable

/**
  * Extensions added to Typesafe config class.
  */
private[kafka] object TypesafeConfigExtensions {

  implicit class RichConfig(val config: Config) extends AnyVal {

    /**
      * Convert Typesafe config to Java `Properties`.
      */
    def toProperties: Properties = {
      val props = new Properties()
      config.entrySet().asScala.foreach(entry => props.put(entry.getKey, entry.getValue.unwrapped().toString))
      props
    }

    /**
      * Convert Typesafe config to a Scala map.
      */
    def toPropertyMap: Map[String, AnyRef] = {
      val map = mutable.Map[String, AnyRef]()
      config.entrySet().asScala.foreach(entry => map.put(entry.getKey, entry.getValue.unwrapped().toString))
      map.toMap
    }
  }
}
