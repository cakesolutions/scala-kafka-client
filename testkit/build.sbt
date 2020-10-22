import Dependencies._

name := "scala-kafka-client-testkit"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % versions.typesafeConfig,

  "org.slf4j" % "slf4j-api" % versions.slf4j,

  "org.apache.kafka" %% "kafka" % versions.kafka
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12"),

  //Test deps
  "org.apache.curator" % "curator-test" % "5.1.0",
  "ch.qos.logback" % "logback-classic" % versions.logback % "test"
)
