package org.apache.gearpump.cluster.main

import java.net.{URL, URLClassLoader}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.gearpump.util.{LogUtil, Constants}

object AppSubmitter extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(AppSubmitter.getClass)

  override val ignoreUnknownArgument = true

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption("<application>.jar mainClass <arguments>", required = true))

  val config = parse(args)
  val jar = config.getString("jar")

  // Set jar path to be submitted to cluster
  System.setProperty(Constants.GEAR_APP_JAR, jar)

  val jarFile = new java.io.File(jar)

  //start main class
  Option(jarFile.exists()) match {
    case Some(true) =>
      val main = config.remainArgs(0)
      val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" + jarFile.getAbsolutePath)),
        Thread.currentThread().getContextClassLoader())
      val clazz = classLoader.loadClass(main)
      val mainMethod = clazz.getMethod("main", classOf[Array[String]])
      mainMethod.invoke(null, config.remainArgs.drop(1))
      
    case Some(false) =>
      LOG.info(s"jar $jar does not exist")
    case None =>
      LOG.info("jar file required")
  }
}
