package cakesolutions.kafka

import javax.management.{MBeanServer, ObjectName}

import cakesolutions.kafka.Health.{Critical, HealthStatus, Ok}

import scala.collection.JavaConverters._

object KafkaHealth {

  /**
    * Create a consumer Health check component to monitor the state of any currently active consumers within the JVM.
    *
    * @param mbeanServer Provide the platforms Mbean server.
    * @param criticalThreshold. A critical will be reported if the connection count is not greater than this value.
    * @return KafkaHealth Component
    */
  def kafkaConsumerHealth(mbeanServer: MBeanServer, criticalThreshold: Double = 0) =
    new KafkaHealth(mbeanServer, criticalThreshold, "kafka.consumer:type=consumer-metrics", "Kafka Consumer")

  /**
    * Create a producer Health check component to monitor the state of any currently active producers within the JVM.
    *
    * @param mbeanServer Provide the platforms Mbean server.
    * @param criticalThreshold. A critical will be reported if the connection count is not greater than this value.
    * @return KafkaHealth Component
    */
  def kafkaProducerHealth(mbeanServer: MBeanServer, criticalThreshold: Double = 0) =
    new KafkaHealth(mbeanServer, criticalThreshold, "kafka.producer:type=producer-metrics", "Kafka Producer")
}

/**
  * A health check component that attempts to provide a status as to the current health of a consumer or producer connection to a Kafka cluster.
  *
  * * @param mbeanServer Provide the platforms Mbean server.
  * @param criticalThreshold A critical will be reported if the connection count is not greater than this value.
  * @param jmxPrefix identifies the JMX property to access Kafka client metrics
  * @param name Name of the component being health checked.
  */
class KafkaHealth(mbeanServer: MBeanServer, criticalThreshold: Double, jmxPrefix: String, name: String) extends HealthStatus {

  private val clientIdProp = "client-id"
  private val connectionCount = "connection-count"
  private val queryName = new ObjectName(s"$jmxPrefix,$clientIdProp=*")

  private def extractClientId(objectName: ObjectName): Either[String, String] =
    Option(objectName.getKeyProperty(clientIdProp)) match {
      case Some(clientId) =>
        Right(clientId)
      case None =>
        Left(s"JMX property $clientIdProp not found in ${objectName.getCanonicalName}")
    }

  private def objectNames =
    mbeanServer.queryNames(queryName, null).asScala

  private def extractConnectionCount(objectName: ObjectName): Either[String, Double] =
    Option(mbeanServer.getAttribute(objectName, connectionCount)) match {
      case Some(d: java.lang.Double) => Right(d)
      case x => Left(s"""Unexpected value for $connectionCount: ${x.getOrElse("null")}""")
    }

  private def getStatus(connCount: Double) =
    if (connCount > criticalThreshold) Ok
    else Critical

  private def health(id: String, connCount: Double) =
    SimpleHealth(getStatus(connCount), s"Number of active connections from $clientIdProp $id is $connCount")

  private def unexpectedErrorHealth(errorMessage: String) =
    SimpleHealth(Critical, errorMessage)

  private def mkHealth(objectName: ObjectName): Health = {
    val h = for {
      clientId <- extractClientId(objectName).right
      count <- extractConnectionCount(objectName).right
    } yield health(clientId, count)

    h.fold(l => unexpectedErrorHealth(l), r => r)
  }

  /**
    * Retrieve the current health of the Kafka consumer/producer
    * @return
    */
  override def getHealth: Health = {
    val nested = objectNames.map(objectName => mkHealth(objectName)).toList
    NestedHealth(name, nested)
  }
}
