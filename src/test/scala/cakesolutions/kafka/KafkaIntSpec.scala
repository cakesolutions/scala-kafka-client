package cakesolutions.kafka

import java.util

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory

class KafkaIntSpec extends KafkaTestServer {

  val log = LoggerFactory.getLogger(getClass)

//  "Ka" should "test" in {
//    val kafkaPort = kafkaServer.kafkaPort
//    log.info("ZK:" + kafkaServer.zookeeperConnect)
//    log.info("!!:" + kafkaServer)
//    log.info("Kafka Port: [{}]", kafkaPort)
//    val consumer = KafkaConsumer(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:" + kafkaPort)
//
//    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
//    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
//    producer.send(KafkaProducerRecord("test", Some("1"), "a"))
//    producer.send(KafkaProducerRecord("test", Some("2"), "a"))
//    producer.flush()
//    log.info("!!!!!!!!")
//
//    val a = new util.ArrayList[String]()
//    a.add("test")
//
//    consumer.subscribe(a)
//    val records = consumer.poll(20000)
//
//    records.count() shouldEqual 2
//
//    producer.close
//    consumer.close
//  }
}
