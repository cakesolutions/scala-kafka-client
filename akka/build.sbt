import Dependencies._

name := "scala-kafka-client-akka"

Defaults.itSettings

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % versions.typesafeConfig,
  "com.typesafe.akka" %% "akka-actor" % versions.akka,

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j % "test",
  "com.typesafe.akka" %% "akka-testkit" % versions.akka % "test",
  "com.typesafe.akka" %% "akka-slf4j" % versions.akka % "test",
  "org.scalatest" %% "scalatest" % versions.scalaTest % "test",
  "org.scalatestplus" %% "mockito-1-10" % "3.1.0.0" % "test",
  "org.mockito" % "mockito-core" % "4.6.1" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)