import AssemblyKeys._

organization := "com.github.intel-hadoop"

name := "gearpump"

version := "0.4-SNAPSHOT"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "maven-repo" at "http://repo.maven.apache.org/maven2",
  "apache-repo" at "https://repository.apache.org/content/repositories/releases",
  "jboss-repo" at "https://repository.jboss.org/nexus/content/repositories/releases",
  "mqtt-repo" at "https://repo.eclipse.org/content/repositories/paho-releases",
  "cloudera-repo" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "mapr-repo" at "http://repository.mapr.com/maven",
  "spring-releases" at "http://repo.spring.io/libs-release"
)

parallelExecution in Test := false

val jettyVersion = "8.1.14.v20131031"
val jgraphtVersion = "0.9.0"
val guavaVersion = "14.0.1"
val akkaVersion = "2.3.4"
val slf4jVersion = "1.7.5"
val commonsLangVersion = "3.3.2"
val commonsHttpVersion = "3.1"

libraryDependencies ++= Seq(
  ("org.eclipse.jetty" % "jetty-plus" % jettyVersion).
  exclude("org.eclipse.jetty.orbit", "javax.servlet").
  exclude("org.eclipse.jetty.orbit", "javax.transaction").
  exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
  exclude("org.eclipse.jetty.orbit", "javax.activation"),
  "org.jgrapht" % "jgrapht-core" % jgraphtVersion,
  ("org.eclipse.jetty" % "jetty-server" % jettyVersion).
  exclude("org.eclipse.jetty.orbit", "javax.transaction").
  exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
  exclude("org.eclipse.jetty.orbit", "javax.activation"),
  "com.google.guava" % "guava" % guavaVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-agent" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
)

seq(assemblySettings: _*)

mergeStrategy in assembly := { 
  case n if n.startsWith("META-INF/eclipse.inf") => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSE_.RSA") => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSEF.SF") => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSE_.SF") => MergeStrategy.discard
  case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case n if n.startsWith("META-INF/NOTICE.txt") => MergeStrategy.discard
  case n if n.startsWith("META-INF/NOTICE") => MergeStrategy.discard
  case n if n.startsWith("META-INF/LICENSE.txt") => MergeStrategy.discard
  case n if n.startsWith("META-INF/LICENSE") => MergeStrategy.discard
  case n if n.startsWith("rootdoc.txt") => MergeStrategy.discard
  case n if n.startsWith("readme.html") => MergeStrategy.discard
  case n if n.startsWith("readme.txt") => MergeStrategy.discard
  case n if n.startsWith("library.properties") => MergeStrategy.discard
  case n if n.startsWith("plugin.properties") => MergeStrategy.discard
  case n if n.startsWith("license.html") => MergeStrategy.discard
  case n if n.startsWith("about.html") => MergeStrategy.discard
  case n if n.startsWith("org/apache/commons/logging/impl/SimpleLog.class") => MergeStrategy.last
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.startsWith("org/apache/commons/logging") => MergeStrategy.last
  case _ => MergeStrategy.deduplicate
}

