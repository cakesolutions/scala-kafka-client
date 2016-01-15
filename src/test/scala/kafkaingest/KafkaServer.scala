package kafkaingest

import java.net.ServerSocket
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.slf4j.LoggerFactory

object KafkaServer {

  protected def kafkaConfig(zkConnectString: String) = {
    //TODO tidy
    val propsI: Iterator[Properties] = createBrokerConfigs(1, zkConnectString).iterator
    assert(propsI.hasNext)
    val props = propsI.next()
    assert(props.containsKey("zookeeper.connect"))
    new KafkaConfig(props)
  }

  /**
   * Create a test config for the given node id
   */
  private def createBrokerConfig(nodeId: Int, port: Int = choosePort(), zookeeperConnect: String,
                                 enableControlledShutdown: Boolean = true): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", "./target/kafka")
    props.put("zookeeper.connect", zookeeperConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("controlled.shutdown.enable", enableControlledShutdown.toString)
    props
  }

  /**
   * Create a test config for the given node id
   */
  private def createBrokerConfigs(numConfigs: Int,
                                 zookeeperConnnect: String,
                                  enableControlledShutdown: Boolean = true): List[Properties] = {
    for ((port, node) <- choosePorts(numConfigs).zipWithIndex)
      yield createBrokerConfig(node, port, zookeeperConnnect, enableControlledShutdown)
  }

  /**
   * Choose an available port
   */
  private def choosePort(): Int = choosePorts(1).head

  /**
   * Choose a number of random available ports
   */
  private def choosePorts(count: Int): List[Int] = {
    val sockets =
      for (i <- 0 until count)
        yield new ServerSocket(0)
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.map(_.close)
    ports
  }
}

//A startable kafka server.  zookeeperPort is generated.
class KafkaServer {

  import KafkaServer._

  val log = LoggerFactory.getLogger(getClass)
  val zookeeperPort = choosePort()
  val zookeeperConnect = "127.0.0.1:" + zookeeperPort

  //Start a zookeeper server
  val zkServer = new TestingServer(zookeeperPort)

  //Build Kafta config with zookeeper connection
  val config = kafkaConfig(zkServer.getConnectString)
  log.info("ZK Connect: " + zkServer.getConnectString)

  // Kafka Test Server
  val kafkaServer = new KafkaServerStartable(config)

  val kafkaPort = config.port

  def startup() = {
    kafkaServer.startup()
    log.info(s"Started kafka on port [${kafkaPort}]")
  }

  def close() = {
    log.info(s"Stopping kafka on port [${kafkaPort}")
    kafkaServer.shutdown()
    zkServer.stop()
  }
}

//Scalatest base class that provides a running kafka instance.
trait KafkaTestServer extends FlatSpec with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll() = {
    kafkaServer.startup()
  }

  override def afterAll() = {
    kafkaServer.close()
  }
}

