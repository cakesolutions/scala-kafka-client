import Dependencies._

name := "scala-kafka-client-examples"

Defaults.itSettings

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % versions.typesafeConfig,
  "com.typesafe.akka" %% "akka-slf4j" % versions.akka,
  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "ch.qos.logback" % "logback-classic" % versions.logback,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j
)