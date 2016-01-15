package kafkaingest

import org.slf4j.LoggerFactory


class KafkaIntSpec extends KafkaTestServer {

  val log = LoggerFactory.getLogger(getClass)

  "Ka" should "test" in {
    val kafkaPort = kafkaServer.kafkaPort
    log.info("ZK:" + kafkaServer.zookeeperConnect)
    log.info("!!:" + kafkaServer)
    log.info("Kafka Port: [{}]", kafkaPort)
    //    Thread.sleep(5000)
    val consumer = KafkaConsumer[String, String](bootstrapServers = "localhost:" + kafkaPort)
    var count = 0

    //TODO block version of call

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    val producer = KafkaProducer[String, String](bootstrapServers = "localhost:" + kafkaPort)
    //    Thread.sleep(15000)
    //
    //    log.info("!!!!!!!!")
    //    producer.send("test", "a", "1")
    //    producer.send("test", "b", "1")
    //    producer.send("test", "c", "1")
    //    producer.send("test", "d", "1")
    //    Thread.sleep(25000)
    //    while (true) {
    log.info("!!::" + count)
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.flush()
    log.info("!!!!!!!!")
    Thread.sleep(20000)
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    log.info("!!" + count)
    //    }
    //    consumer.consume("test") { (_,_) => count += 1 }
    //    consumer.consume("test") { (_,_) => count += 1 }

    count shouldEqual 1

    //    producer.close
    consumer.close
  }
}
