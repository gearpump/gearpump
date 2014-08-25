name := "gearpump"

version := "0.4-SNAPSHOT"

organization := "com.github.intel-hadoop"

scalaVersion := "2.10.4"

packSettings

packMain := Map("local" -> "org.apache.gears.cluster.Local",
                "master" -> "org.apache.gears.cluster.Master",
                "worker" -> "org.apache.gears.cluster.Worker",
                "sol" -> "org.apache.gearpump.examples.sol.SOL",
                "wordcount" -> "org.apache.gearpump.examples.wordcount.WordCount")
				
packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")		
		
packExtraClasspath := Map("conf" -> Seq("${PROG_HOME}/conf"))

resolvers ++= Seq(
  "maven-repo" at "http://repo.maven.apache.org/maven2",
  "maven1-repo" at "http://repo1.maven.org/maven2",
  "apache-repo" at "https://repository.apache.org/content/repositories/releases",
  "jboss-repo" at "https://repository.jboss.org/nexus/content/repositories/releases",
  "spray repo" at "http://repo.spray.io/spray",
  "mqtt-repo" at "https://repo.eclipse.org/content/repositories/paho-releases",
  "cloudera-repo" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "mapr-repo" at "http://repository.mapr.com/maven",
  "spring-releases" at "http://repo.spring.io/libs-release",
  "sonatype" at "https://oss.sonatype.org/content/repositories/releases"
)

parallelExecution in Test := false

val akkaVersion = "2.3.4"
val kyroVersion = "0.3.2"
val codahaleVersion = "3.0.2"
val commonsLangVersion = "3.3.2"
val commonsHttpVersion = "3.1"
val guavaVersion = "14.0.1"
val jettyVersion = "8.1.14.v20131031"
val jgraphtVersion = "0.9.0"
val parboiled2Version = "2.0.0"
val slf4jVersion = "1.7.5"
val sprayVersion = "1.3.1"
val sprayJsonVersion = "1.2.6"

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
  "org.parboiled" %% "parboiled" % parboiled2Version,
  "io.spray" %%  "spray-can" % sprayVersion,
  "io.spray" %%  "spray-http" % sprayVersion,
  "io.spray" %%  "spray-routing-shapeless2" % sprayVersion,
  "io.spray" %%  "spray-json" % sprayJsonVersion,
  "com.google.guava" % "guava" % guavaVersion,
  "com.codahale.metrics" % "metrics-core" % codahaleVersion,
  "com.codahale.metrics" % "metrics-graphite" % codahaleVersion,
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
  "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
  "org.apache.commons" % "commons-lang3" % commonsLangVersion,
  "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-agent" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.github.romix.akka" %% "akka-kryo-serialization" % kyroVersion
)
