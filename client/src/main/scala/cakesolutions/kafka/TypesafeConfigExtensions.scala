package cakesolutions.kafka

import java.util.Properties
import com.typesafe.config.Config
import scala.collection.JavaConversions._
import scala.collection.immutable._
import scala.collection.mutable

object TypesafeConfigExtensions {

  implicit class RichConfig(val config: Config) extends AnyVal {
    def toProperties: Properties = {
      val props = new Properties()
      config.entrySet().foreach(entry => props.put(entry.getKey, entry.getValue.unwrapped().toString))
      props
    }

    def toPropertyMap: Map[String, AnyRef] = {
      val map = mutable.Map[String, AnyRef]()
      config.entrySet().foreach(entry => map.put(entry.getKey, entry.getValue.unwrapped().toString))
      map.toMap
    }
  }
}
