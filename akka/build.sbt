import Dependencies._

name := "scala-kafka-client-akka"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "com.typesafe.akka" %% "akka-actor" % versions.akka,

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j,
  "org.scala-lang" % "scala-reflect" % "2.11.7",

  "com.typesafe.akka" % "akka-testkit_2.11" % versions.akka % "test",
  "org.scalatest" % "scalatest_2.11" % versions.scalaTest % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)