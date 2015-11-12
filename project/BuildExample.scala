import sbt.Keys._
import sbt._
import Build._
import sbtassembly.Plugin.AssemblyKeys._

object BuildExample extends sbt.Build {

  lazy val examples = Project(
    id = "gearpump-examples",
    base = file("examples"),
    settings = commonSettings ++ noPublish
  ) aggregate (wordcount, wordcountJava, complexdag, sol, fsio, examples_kafka, distributedshell, stockcrawler, transport, examples_state, pagerank, distributeservice)

  lazy val wordcountJava = Project(
    id = "gearpump-examples-wordcountjava",
    base = file("examples/streaming/wordcount-java"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
      Seq(
        mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.wordcountjava.WordCount"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value)
      )
  ) dependsOn (streaming % "test->test; provided", daemon % "test->test; provided")

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/streaming/wordcount"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.wordcount.WordCount"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn (streaming % "test->test; provided", daemon % "test->test; provided")

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/streaming/sol"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.sol.SOL"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)

        )
  ) dependsOn (streaming % "test->test; provided")

  lazy val complexdag = Project(
    id = "gearpump-examples-complexdag",
    base = file("examples/streaming/complexdag"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.complexdag.Dag"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn (streaming % "test->test; provided")

  lazy val transport = Project(
    id = "gearpump-examples-transport",
    base = file("examples/streaming/transport"),
    settings = commonSettings ++ noPublish ++ 
        Seq(
          libraryDependencies ++= Seq(
            "io.spray" %%  "spray-can"       % sprayVersion,
            "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
            "io.spray" %%  "spray-json"    % sprayJsonVersion,
            "com.lihaoyi" %% "upickle" % upickleVersion
          ),
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.transport.Transport"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn (streaming % "test->test; provided")

  lazy val distributedshell = Project(
    id = "gearpump-examples-distributedshell",
    base = file("examples/distributedshell"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          mainClass in (Compile, packageBin) := Some("io.gearpump.examples.distributedshell.DistributedShell"),
          target in assembly := baseDirectory.value.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn(daemon % "test->test; provided")

  lazy val distributeservice = Project(
    id = "gearpump-examples-distributeservice",
    base = file("examples/distributeservice"),
    settings = commonSettings ++ noPublish ++ 
        Seq(
          libraryDependencies ++= Seq(
            "commons-lang" % "commons-lang" % commonsLangVersion,
            "commons-io" % "commons-io" % commonsIOVersion,
            "io.spray" %%  "spray-can"       % sprayVersion,
            "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion
          ),
          mainClass in (Compile, packageBin) := Some("io.gearpump.experiments.distributeservice.DistributeService"),
          target in assembly := baseDirectory.value.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
    )
  ) dependsOn(daemon % "test->test; provided")

  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/streaming/fsio"),
    settings = commonSettings ++ noPublish ++
        Seq(
          libraryDependencies ++= Seq(
            "org.apache.hadoop" % "hadoop-common" % clouderaVersion
              exclude("org.mortbay.jetty", "jetty-util")
              exclude("org.mortbay.jetty", "jetty")
              exclude("org.fusesource.leveldbjni", "leveldbjni-all")
              exclude("tomcat", "jasper-runtime")
              exclude("commons-beanutils", "commons-beanutils-core")
              exclude("commons-beanutils", "commons-beanutils")
              exclude("asm", "asm")
              exclude("org.ow2.asm", "asm")
          ),
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.fsio.SequenceFileIO"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn (streaming % "test->test; provided")

  lazy val examples_kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/streaming/kafka"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.kafka.wordcount.KafkaWordCount"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn(streaming % "test->test; provided", external_kafka)

  lazy val stockcrawler = Project(
    id = "gearpump-examples-stockcrawler",
    base = file("examples/streaming/stockcrawler"),
    settings = commonSettings ++ noPublish ++ 
        Seq(
          libraryDependencies ++= Seq(
            "io.spray" %%  "spray-can"       % sprayVersion,
            "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
            "com.lihaoyi" %% "upickle" % upickleVersion,
            "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
            "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.2",
            "joda-time" % "joda-time" % "2.7",
            "io.spray" %%  "spray-json"    % sprayJsonVersion
          ),
          mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.stock.main.Stock"),
          target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn (streaming % "test->test; provided", external_kafka % "test->test")

  lazy val examples_state = Project(
    id = "gearpump-examples-state",
    base = file("examples/streaming/state"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++ Seq(
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-common" % clouderaVersion
          exclude("org.mortbay.jetty", "jetty-util")
          exclude("org.mortbay.jetty", "jetty")
          exclude("org.fusesource.leveldbjni", "leveldbjni-all")
          exclude("tomcat", "jasper-runtime")
          exclude("commons-beanutils", "commons-beanutils-core")
          exclude("commons-beanutils", "commons-beanutils")
          exclude("asm", "asm")
          exclude("org.ow2.asm", "asm"),
        "org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion
      ),
      mainClass in (Compile, packageBin) := Some("io.gearpump.streaming.examples.state.MessageCountApp"),
      target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value)
    )
  ) dependsOn (streaming % "test->test; provided", external_hadoopfs, external_monoid, external_serializer)

  lazy val pagerank = Project(
    id = "gearpump-examples-pagerank",
    base = file("examples/pagerank"),
    settings = commonSettings ++ noPublish ++ Seq(
      mainClass in (Compile, packageBin) := Some("io.gearpump.experiments.pagerank.example.PageRankExample"),
      target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value)
    )
  ) dependsOn(streaming % "test->test; provided")
}
