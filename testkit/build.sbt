import Dependencies._

name := "scala-kafka-client-testkit"

libraryDependencies ++= Seq(
  //  "com.typesafe.akka" %% "akka-actor" % akkaVersion
  "com.typesafe" % "config" % "1.3.0",

  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j,
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.apache.kafka" %% "kafka" % versions.kafka
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12"),

  //Test deps
  //  "org.scalatest" % "scalatest_2.11" % versions.scalaTest % "test",
  "org.apache.curator" % "curator-test" % "2.7.0" //3.0.0
)