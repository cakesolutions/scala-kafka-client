name := "scala-kafka-client"
organization := "net.cakesolutions"

version := "0.1.0"

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

publishMavenStyle := true

bintrayOrganization := Some("simonsouter")

licenses := ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.txt")) :: Nil

val publishSettings =
//TODO publis snapshots to OSS
//  if (Version.endsWith("-SNAPSHOT"))
//    Seq(
//      publishTo := Some("Artifactory Realm" at "http://oss.jfrog.org/artifactory/oss-snapshot-local"),
//      bintrayReleaseOnPublish := false,
//      // Only setting the credentials file if it exists (#52)
//      credentials := List(Path.userHome / ".bintray" / ".artifactory").filter(_.exists).map(Credentials(_))
//    )
//  else
  Seq(
    pomExtra := <scm>
      <url>git@github.com:simonsouter/scala-kafka-client.git</url>
      <connection>scm:git:git@github.com:simonsouter/scala-kafka-client.git</connection>
    </scm>
      <developers>
        <developer>
          <id>simon</id>
          <name>Simon Souter</name>
          <url>https://github.com/simonsouter</url>
        </developer>
      </developers>,
    publishArtifact in Test := false
  )