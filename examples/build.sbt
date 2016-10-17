import Dependencies._

name := "examples"

Defaults.itSettings

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j
)