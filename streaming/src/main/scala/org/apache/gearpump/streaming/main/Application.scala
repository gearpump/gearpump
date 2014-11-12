package org.apache.gearpump.cluster.streaming

import java.net.{URL, URLClassLoader}

import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.streaming.client.Starter
import org.slf4j.{Logger, LoggerFactory}

object Application extends App with ArgumentsParser {
  val LOG: Logger = LoggerFactory.getLogger(Application.getClass)

  override val ignoreUnknownArgument = true

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption("<application>.jar mainClass <arguments>", required = true))

  val config = parse(args)
  val jar = config.getString("jar")
  val jarFile = new java.io.File(jar)
  Option(jarFile.exists()) match {
    case Some(true) =>
      val main = config.remainArgs(0)
      val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" + jarFile.getAbsolutePath)),
        Thread.currentThread().getContextClassLoader())
      val instance = classLoader.loadClass(main).newInstance()
      val start = instance.asInstanceOf[Starter]
      start.main(config.remainArgs.drop(1))
    case Some(false) =>
    case None =>
  }
}
