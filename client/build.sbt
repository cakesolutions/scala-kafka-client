import Dependencies._

name := "scala-kafka-client"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j,

  //Test deps
  "org.scalatest" % "scalatest_2.11" % versions.scalaTest % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)