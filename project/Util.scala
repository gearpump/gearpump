import sbt._
import sbt.Keys._
import java.nio.file.Files
import java.util.regex.Pattern
import scala.collection.JavaConverters._

object Util {
  lazy val copySharedResources = Def.task {
//    Called when we have files under core, streaming that need to be copyied into services/js.
//    ConsoleLogger().info("copying shared resources")
//    val sharedMessages = "src/main/scala/org/apache/gearpump/shared/Messages.scala"
//    IO.copyFile(core.base / sharedMessages, file(Build.servicesjs.baseDirectory) / sharedMessages)
  }

  def findDirs(currentDir: String): Array[File] = {
    val dirs = IO.listFiles(file(currentDir),  DirectoryFilter)
    dirs ++ dirs.flatMap(dir => {
      val childDir = currentDir + "/" + dir.getName
      findDirs(file(childDir).getPath)
    })
  }

  def findFiles(dir: File, pattern: PatternFilter): Array[File] = {
    val dirs = findDirs(dir.getPath)
    IO.listFiles(dir, pattern) ++ dirs.flatMap(dir => {
      IO.listFiles(dir, pattern)
    })
  }

  def copyJSArtifactsToOutput: Unit = {
    ConsoleLogger().info(s"copying JS artifacts ${Build.servicesjs.base.getPath}")
    val in = file(s"${Build.servicesjs.base.getPath}/target/${Build.scalaVersionMajor}/gearpump-services-js-fastopt.js.map")
    val out = file(s"${Build.distDashboardDirectory}/gearpump-services-js-fastopt.js.map")
    val input = IO.read(in)
    val data = input.replaceAll("../../src","src")
    IO.write(out, data)
    IO.copyFile(
      file(s"${Build.servicesjs.base.getPath}/target/${Build.scalaVersionMajor}/gearpump-services-js-fastopt.js"),
      file(s"${Build.distDashboardDirectory}/gearpump-services-js-fastopt.js")
    )
    IO.copyDirectory(
      file(s"${Build.servicesjs.base.getPath}/src"),
      file(s"${Build.distDashboardDirectory}"),
      true
    )
    IO.copyDirectory(
      file(Build.servicesjs.base.getPath),
      file(Build.distDashboardDirectory),
      true
    )
    val htmlFiles = findFiles(file(s"${Build.services.base.getPath}/dashboard"),
      new PatternFilter(Pattern.compile("^.*.html$")))
    htmlFiles.foreach(htmlFile => {
      val source = htmlFile.getPath
      val target = s"${Build.distDirectory}/target/pack" + 
        htmlFile.getPath.replaceFirst(s"${Build.services.base.getPath}/", "/")
      IO.copyFile(file(source), file(target))
    })
  }
}
