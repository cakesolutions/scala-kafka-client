package cakesolutions.kafka

import org.scalatest.{FlatSpec, Matchers}

class KafkaSpec extends FlatSpec with Matchers {

//  "Kafka Producer" should "Produce" in {
//    val producer = KafkaProducer[String, String]()
//
//    (1 to 1000).foreach(i => producer.send("test", i.toString, "abc"))
//  }
//
//  "Kafka Consumer" should "Consume" in {
//    val consumer = KafkaConsumer[String,String]()
//
//    (1 to 1000).foreach(i => {
//      println("!!Consume")
//      consumer.consume("test") { (key, value) =>
//        println("k:" + key + ". v:" + value)
//      }
//      Thread.sleep(1000)
//    })
//
//    consumer.close
//  }
}
