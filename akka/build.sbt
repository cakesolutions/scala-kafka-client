import Dependencies._

name := "scala-kafka-client-akka"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % versions.akka,
  "com.typesafe" % "config" % "1.3.0",

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  //  "org.apache.kafka" %% "kafka" % versions.kafka
  //    exclude("log4j", "log4j")
  //    exclude("org.slf4j", "slf4j-log4j12"),
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j,
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scala-lang" % "scala-reflect" % "2.11.7",

  //Test deps
  "org.scalatest" % "scalatest_2.11" % versions.scalaTest % "test"
)