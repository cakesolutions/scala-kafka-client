logLevel := Level.Warn

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.9")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.1")

addSbtPlugin("com.updateimpact" % "updateimpact-sbt-plugin" % "2.1.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

resolvers += "Artifactory Realm" at "https://iadvize.jfrog.io/iadvize/iadvize-sbt"

credentials += Credentials(
  "Artifactory Realm",
  "iadvize.jfrog.io",
  (sys.env.get("ARTIFACTORY_USERNAME") orElse sys.props.get("ARTIFACTORY_USERNAME")).getOrElse(""),
  (sys.env.get("ARTIFACTORY_PASS") orElse sys.props.get("ARTIFACTORY_PASS")).getOrElse("")
)

addSbtPlugin("com.iadvize"       % "sbt-iadvize-plugin"  % "4.6.2")
