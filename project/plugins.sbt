addSbtPlugin("org.scala-js" % "sbt-scalajs" % "0.6.3")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.6.9")

addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.1.6")

addSbtPlugin("com.gilt" % "sbt-dependency-graph-sugar" % "0.7.4")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "0.2.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.5")

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "3.0.0")
