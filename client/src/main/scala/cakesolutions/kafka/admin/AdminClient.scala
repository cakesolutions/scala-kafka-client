package cakesolutions.kafka.admin

import cakesolutions.kafka.TypesafeConfigExtensions._
import com.typesafe.config.Config
import org.apache.kafka.clients.admin.{AdminClientConfig, NewPartitions, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.clients.{CommonClientConfigs, admin}
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigResource

import scala.collection.JavaConverters._

object AdminClient {
  def apply(conf: Conf): AdminClient =
    new AdminClient(JAdminClient.create(conf.props.asJava))

  object Conf {

    def apply(bootstrapServers: String = "localhost:9092",
      clientId: String = "",
      metadataMaxAge: Int = 5 * 60 * 1000,
      sendBufferBytes: Int = 5 * 60 * 1000,
      receiveBufferBytes: Int = 64 * 1024,
      reconnectBackoffMs: Long = 50L,
      retryBackoffMs: Long = 1000L,
      requestTimeoutMs: Int = 120000,
      connectionsMaxIdleMs: Int = 5 * 60 * 1000,
      securityProtocol: String = CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL): Conf = {

      val configMap = Map[String, AnyRef](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        AdminClientConfig.CLIENT_ID_CONFIG -> clientId,
        AdminClientConfig.METADATA_MAX_AGE_CONFIG -> metadataMaxAge.toString,
        AdminClientConfig.SEND_BUFFER_CONFIG -> sendBufferBytes.toString,
        AdminClientConfig.RECEIVE_BUFFER_CONFIG -> receiveBufferBytes.toString,
        AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG -> receiveBufferBytes.toString,
        AdminClientConfig.RETRY_BACKOFF_MS_CONFIG -> retryBackoffMs.toString,
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> requestTimeoutMs.toString,
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> connectionsMaxIdleMs.toString,
        AdminClientConfig.SECURITY_PROTOCOL_CONFIG -> securityProtocol
      )

      apply(configMap)
    }

    def apply(config: Config): Conf = apply(config.toPropertyMap)
  }

  final case class Conf(props: Map[String, AnyRef])
}

final class AdminClient(client: JAdminClient) {

  def close(): Unit = client.close()

  def createTopics(newTopics: List[NewTopic]) = client.createTopics(newTopics.asJava)

  def deleteTopics(topics: List[String]) = client.deleteTopics(topics.asJava)

  def listTopics() = client.listTopics()

  def describeTopics(topicNames: List[String]) = client.describeTopics(topicNames.asJava)

  def describeCluster() = client.describeCluster()

  def describeAcls(filter: AclBindingFilter) = client.describeAcls(filter)

  def createAcls(acls: List[AclBinding]) = client.createAcls(acls.asJava)

  def deleteAcls(filters: List[AclBindingFilter]) = client.deleteAcls(filters.asJava)

  def describeConfigs(resources: List[ConfigResource]) = client.describeConfigs(resources.asJava)

  def alterConfigs(configs: Map[ConfigResource, admin.Config]) = client.alterConfigs(configs.asJava)

  def alterReplicaLogDirs(replicaAssignment: Map[TopicPartitionReplica, String]) = client.alterReplicaLogDirs(replicaAssignment.asJava)

  def describeLogDirs(brokers: List[Integer]) = client.describeLogDirs(brokers.asJava)

  def describeReplicaLogDirs(replicas: List[TopicPartitionReplica]) = client.describeReplicaLogDirs(replicas.asJava)

  def createPartitions(newPartitions: Map[String, NewPartitions]) = client.createPartitions(newPartitions.asJava)
}
