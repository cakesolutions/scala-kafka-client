package cakesolutions.kafka

import java.util.Properties
import com.typesafe.config.Config
import scala.collection.JavaConversions._

object TypesafeConfigExtensions {

  implicit class RichConfig(val config: Config) extends AnyVal {
    def toProperties: Properties = {
      val props = new Properties()
      config.entrySet().foreach(entry => props.put(entry.getKey, entry.getValue.unwrapped().toString))
      props
    }
  }
}
