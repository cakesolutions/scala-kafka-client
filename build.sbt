lazy val commonSettings = Seq(
  organization := "net.cakesolutions",
  scalaVersion := "2.11.7",
  publishMavenStyle := true,
  bintrayOrganization := Some("simonsouter"),
  bintrayPackageLabels := Seq("scala", "kafka"),

//  publishTo :=
  //TODO publish snapshots to OSS
  //  if (Version.endsWith("-SNAPSHOT"))
  //    Seq(
  //      publishTo := Some("Artifactory Realm" at "http://oss.jfrog.org/artifactory/oss-snapshot-local"),
  //      bintrayReleaseOnPublish := false,
  //      // Only setting the credentials file if it exists (#52)
  //      credentials := List(Path.userHome / ".bintray" / ".artifactory").filter(_.exists).map(Credentials(_))
  //    )
  //  else

//  parallelExecution in Test := false,

  publishArtifact in Test := false,

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

  licenses := ("MIT", url("http://opensource.org/licenses/MIT")):: Nil
)

lazy val kafkaTestkit = project.in(file("testkit"))
  .settings(commonSettings: _*)

lazy val scalaKafkaClient = project.in(file("client")).dependsOn(kafkaTestkit % "test")
  .settings(commonSettings: _*)

lazy val scalaKafkaClientAkka = project.in(file("akka")).dependsOn(scalaKafkaClient).dependsOn(kafkaTestkit % "test")
  .settings(commonSettings: _*)

