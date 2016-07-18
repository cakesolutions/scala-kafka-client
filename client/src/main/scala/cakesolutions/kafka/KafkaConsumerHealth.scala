package cakesolutions.kafka

import javax.management.{MBeanServer, ObjectName}

import cakesolutions.kafka.Health.{Critical, HealthStatus, Ok, Warning}

import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaConsumerHealth(mbeanServer: MBeanServer, warningThreshold: Double, criticalThreshold: Double) extends HealthStatus {

  private val clientIdProp = "client-id"
  private val lastHeartBeatAttr = "last-heartbeat-seconds-ago"

  private def extractClientId(objectName: ObjectName): Either[String, String] =
    Option(objectName.getKeyProperty(clientIdProp)) match {
      case Some(clientId) =>
        Right(clientId)
      case None =>
        Left(s"JMX property $clientIdProp not found in ${objectName.getCanonicalName}")
    }

  private def coordMetricsConsumerObjectNames: mutable.Set[ObjectName] =
    mbeanServer.queryNames(new ObjectName(s"kafka.consumer:type=consumer-coordinator-metrics,$clientIdProp=*"), null).asScala

  private def extractLastHeartbeat(objectName: ObjectName): Either[String, Double] =
    Option(mbeanServer.getAttribute(objectName, lastHeartBeatAttr)) match {
      case Some(d: java.lang.Double) => Right(d)
      case x => Left(s"""Unexpected value for $lastHeartBeatAttr: ${x.getOrElse("null")}""")
    }

  private def getStatus(lastHeartbeat: Double) =
    if (lastHeartbeat < 0) Critical
    else if (lastHeartbeat < warningThreshold) Ok
    else if (lastHeartbeat < criticalThreshold) Warning
    else Critical

  private def health(id: String, lastHeartbeat: Double) =
    SimpleHealth(getStatus(lastHeartbeat), s"Last heartbeat from $clientIdProp $id was $lastHeartbeat seconds ago")

  private def unexpectedErrorHealth(errorMessage: String) =
    SimpleHealth(Critical, errorMessage)

  private def mkHealth(objectName: ObjectName): Health = {
    val h = for {
      clientId <- extractClientId(objectName).right
      lastHeartbeat <- extractLastHeartbeat(objectName).right
    } yield health(clientId, lastHeartbeat)

    h.fold(l => unexpectedErrorHealth(l), r => r)
  }

  override def getHealth: Health = {
    val nested = coordMetricsConsumerObjectNames.map(objectName => mkHealth(objectName)).toList
    NestedHealth("Kafka Consumer", nested)
  }
}