name := "scala-kafka-client"

organization := "net.cakesolutions"

version := "1.0"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.0"
val slf4jVersion = "1.7.12"

libraryDependencies ++= Seq(
//  "com.typesafe.akka" %% "akka-actor" % akkaVersion
  "com.typesafe" % "config" % "1.3.0",

  "org.apache.kafka" % "kafka-clients" % "0.9.0.0",
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  "ch.qos.logback" % "logback-classic" % "1.1.3",

  //Test deps
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M14" % "test",
  "org.apache.kafka" %% "kafka" % "0.9.0.0" % "test"
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.curator" % "curator-test" % "2.7.0" % "test" //3.0.0
)
