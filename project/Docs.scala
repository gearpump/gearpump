import BuildGearpump.{core, streaming}
import sbt.Keys._
import sbt._
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin._

object Docs extends sbt.Build {
  lazy val javadocSettings = Seq(
    addCompilerPlugin(
      "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.11" cross CrossVersion.full),
    scalacOptions += s"-P:genjavadoc:out=${target.value}/java"
  )

  lazy val gearpumpUnidocSetting = scalaJavaUnidocSettings ++ Seq(
    unidocProjectFilter in(ScalaUnidoc, unidoc) := projectsWithDoc,
    unidocProjectFilter in(JavaUnidoc, unidoc) := projectsWithDoc,

    unidocAllSources in(ScalaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in(ScalaUnidoc, unidoc)).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in(JavaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
    }
  )

  private lazy val projectsWithDoc = {
    val projects: Seq[ProjectReference] = Seq[ProjectReference](
      core,
      streaming
    )
    inProjects(projects: _*)
  }

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages
      .map(_.filterNot(_.getName.contains("$")))
      .map(_.filterNot(_.getCanonicalPath.contains("akka")))
  }
}
