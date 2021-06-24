import ReleaseTransformations._
import xerial.sbt.Sonatype._

lazy val commonSettings = Seq(
  organization := "com.pirum",
  organizationName := "Pirum Systems",
  organizationHomepage := Some(url("https://pirum.com")),
  pomIncludeRepository := { _ => false },
  scalaVersion := "2.12.12",
  crossScalaVersions := Seq("2.12.12", "2.13.5"),
  //  resolvers += "Apache Staging" at "https://repository.apache.org/content/groups/staging/",
  resolvers += Resolver.bintrayRepo("mockito", "maven"),
  Compile / scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8",
    "-feature",
    "-deprecation",
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-unused"
  ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 13)) => Seq()
    case _             => Seq("-Xfuture", "-Ywarn-unused-import", "-Ywarn-nullary-unit")
  }),
  Compile / doc / scalacOptions ++= Seq("-groups", "-implicits"),
  Compile / doc / javacOptions ++= Seq("-notimestamp", "-linksource"),
  Test / parallelExecution := false,
  IntegrationTest / parallelExecution := true,
  autoAPIMappings := true,
  publishTo := sonatypePublishToBundle.value,
  Test / publishArtifact := false,
  publishMavenStyle := true,
  licenses := Seq(
    "MIT" -> url(
      "https://github.com/Pirum-Systems/scala-kafka-client/blob/master/LICENSE.txt"
    )
  ),
  sonatypeProjectHosting := Some(
    GitHubHosting(
      "Pirum-Systems",
      "scala-kafka-client",
      "regis.kuckaertz@pirum.com"
    )
  ),
  sonatypeCredentialHost := "s01.oss.sonatype.org",
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/Pirum-Systems/scala-kafka-client"),
      "scm:git@github.com:Pirum-Systems/scala-kafka-client.git"
    )
  ),
  developers := List(
    Developer(
      id = "regiskuckaertz",
      name = "Regis Kuckaertz",
      email = "",
      url = url("https://github.com/regiskuckaertz")
    ),
    Developer(
      id = "simon",
      name = "Simon Souter",
      email = "",
      url = url("https://github.com/simonsouter")
    ),
    Developer(
      id = "jkpl",
      name = "aakko Pallari",
      email = "",
      url = url("https://github.com/jkpl")
    )
  )
)

val releaseSettings = List(
  releaseCrossBuild := true,
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"),
    releaseStepCommand("sonatypeBundleRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val kafkaTestkit = project
  .in(file("testkit"))
  .settings(commonSettings: _*)

lazy val scalaKafkaClient = project
  .in(file("client"))
  .settings(commonSettings: _*)
  .dependsOn(kafkaTestkit % "test")
  .configs(IntegrationTest extend Test)

lazy val scalaKafkaClientAkka = project
  .in(file("akka"))
  .settings(commonSettings: _*)
  .dependsOn(scalaKafkaClient)
  .dependsOn(kafkaTestkit % "test")
  .configs(IntegrationTest extend Test)

lazy val scalaKafkaClientExamples = project
  .in(file("examples"))
  .settings(commonSettings: _*)
  .dependsOn(scalaKafkaClientAkka)

lazy val root = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(releaseSettings: _*)
  .enablePlugins(ScalaUnidocPlugin)
  .settings(
    name := "scala-kafka-client-root",
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
  .aggregate(scalaKafkaClient, scalaKafkaClientAkka, kafkaTestkit)
