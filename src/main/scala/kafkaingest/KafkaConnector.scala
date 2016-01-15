package kafkaingest
//
//import java.util.Properties
//
//import com.typesafe.config.Config
//import org.apache.kafka.clients.producer.KafkaProducer
//
//object KafkaConnector {
//  def apply(props: Properties) = KafkaConnector(props)
//
//  def apply(bootstrapServers: String = "localhost:9092",
//            groupId: String = "test",
//            enableAutoCommit: Boolean = true,
//            autoCommitInterval: Int = 1000,
//            sessionTimeoutMs: Int = 30000,
//            keyDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer",
//            valueDeserializer: String = "org.apache.kafka.common.serialization.StringDeserializer"
//             ): KafkaConnector = {
//    val props = new Properties()
//    props.put("bootstrap.servers", bootstrapServers);
//    props.put("group.id", groupId);
//    props.put("enable.auto.commit", enableAutoCommit.toString);
//    props.put("auto.commit.interval.ms", autoCommitInterval.toString);
//    props.put("session.timeout.ms", sessionTimeoutMs.toString);
//    props.put("key.deserializer", keyDeserializer);
//    props.put("value.deserializer", valueDeserializer);
//
//    KafkaConnector(props)
//  }
//  //TODO based on config
//  def apply(config:Config) = ???
//
//}
//
//case class KafkaConnector(props:Properties) {}
//
//
//
//class KafkaConsumer(connector: KafkaConnector) {
//  val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(connector.props)
//}
//
//class KafkaProducer(connector: KafkaConnector) {
//  val consumer = new KafkaProducer[](connector.props)
//}