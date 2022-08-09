logLevel := Level.Warn

resolvers ++= Seq(
  Resolver.url(
    "Artifactory ivy",
    url("https://artifactory.internal.livongo.com/artifactory/plugins-release-local")
  )(Resolver.ivyStylePatterns)
)

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

//addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.4")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.10")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.1")

addSbtPlugin("com.updateimpact" % "updateimpact-sbt-plugin" % "2.1.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("livongo" %% "build-plugins" % "4.2.0")
