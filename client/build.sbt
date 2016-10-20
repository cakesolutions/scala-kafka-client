import Dependencies._

name := "scala-kafka-client"

Defaults.itSettings

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % versions.typesafeConfig,

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,

  //Test deps
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j % "test",
  "org.scalatest" %% "scalatest" % versions.scalaTest % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)