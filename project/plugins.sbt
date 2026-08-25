resolvers += Classpaths.sbtPluginReleases

libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.4.1")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.23")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.2")

addSbtPlugin("com.github.sbt" % "sbt-license-report" % "1.10.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.13")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")

addSbtPlugin("org.scalastyle" % "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt.junit" % "sbt-jupiter-interface" % "0.19.0")
