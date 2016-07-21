package cakesolutions.kafka

import javax.management.{MBeanServer, ObjectName}

import cakesolutions.kafka.Health.{Critical, HealthStatus, Ok, Warning}

import scala.collection.JavaConverters._

class KafkaHealth(mbeanServer: MBeanServer, warningThreshold: Double, criticalThreshold: Double, jmxPrefix: String, name: String) extends HealthStatus {

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
    if (connCount > warningThreshold) Ok
    else if (connCount > criticalThreshold) Warning
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

  override def getHealth: Health = {
    val nested = objectNames.map(objectName => mkHealth(objectName)).toList
    NestedHealth(name, nested)
  }
}

object KafkaHealth {
  def kafkaConsumerHealth(mbeanServer: MBeanServer, warningThreshold: Double, criticalThreshold: Double) =
    new KafkaHealth(mbeanServer, warningThreshold, criticalThreshold, "kafka.consumer:type=consumer-metrics", "Kafka Consumer")

  def kafkaProducerHealth(mbeanServer: MBeanServer, warningThreshold: Double, criticalThreshold: Double) =
    new KafkaHealth(mbeanServer, warningThreshold, criticalThreshold, "kafka.producer:type=producer-metrics", "Kafka Producer")

}