import xerial.sbt.Sonatype._

sonatypeProfileName := "com.pirum"

publishMavenStyle := true

licenses := ("MIT", url("http://opensource.org/licenses/MIT")) :: Nil

sonatypeProjectHosting := Some(
  GitHubHosting(
    "Pirum-Systems",
    "scala-kafka-client",
    "regis.kuckaertz@pirum.com"
  )
)
