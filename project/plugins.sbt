// Custom Plugin Resolvers
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

// Plugins
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.1.0")
addSbtPlugin("de.cellular" % "sbt-git-version" % "1.4.0")
addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
