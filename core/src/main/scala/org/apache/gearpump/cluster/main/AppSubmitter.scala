package org.apache.gearpump.cluster.main

import java.net.{URL, URLClassLoader}

import org.apache.gearpump.util.{Constants, LogUtil, Util}
import org.slf4j.Logger

object AppSubmitter extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override val ignoreUnknownArgument = true

  override val options: Array[(String, CLIOption[Any])] = Array(
    "namePrefix" -> CLIOption[String]("application name prefix", required = false),
    "jar" -> CLIOption("<application>.jar", required = true))

  override val remainArgs = Array(
    "mainClass <remain arguments>"
  )

  def start : Unit = {

    val config = parse(args)
    if (null == config) {
      return
    }

    val jar = config.getString("jar")

    // Set jar path to be submitted to cluster
    System.setProperty(Constants.GEARPUMP_APP_JAR, jar)

    if (config.exists("namePrefix")) {
      val namePrefix = config.getString("namePrefix")
      if (!Util.validApplicationName(namePrefix)) {
        throw new Exception(s"$namePrefix is not a valid prefix for an application name")
      }
      System.setProperty(Constants.GEARPUMP_APP_NAME_PREFIX, namePrefix)
    }

    val jarFile = new java.io.File(jar)

    //start main class
    Option(jarFile.exists()) match {
      case Some(true) =>
        val main = config.remainArgs(0)
        val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" + jarFile.getAbsolutePath)),
          Thread.currentThread().getContextClassLoader())

        //set the context classloader as ActorSystem will use context classloader in precedence.
        Thread.currentThread().setContextClassLoader(classLoader)
        val clazz = classLoader.loadClass(main)
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, config.remainArgs.drop(1))

      case Some(false) =>
        LOG.info(s"jar $jar does not exist")
      case None =>
        LOG.info("jar file required")
    }
  }

  start
}
