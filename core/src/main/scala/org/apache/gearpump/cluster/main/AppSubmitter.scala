package org.apache.gearpump.cluster.main

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.net.{URL, URLClassLoader}

import org.apache.gearpump.cluster.AppJar
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable

object AppSubmitter extends App with ArgumentsParser {
  val LOG: Logger = LoggerFactory.getLogger(AppSubmitter.getClass)

  override val ignoreUnknownArgument = true

  override val options: Array[(String, CLIOption[Any])] = Array(
    "jar" -> CLIOption("<application>.jar mainClass <arguments>", required = true))

  val config = parse(args)
  val jar = config.getString("jar")
  val jarFile = new java.io.File(jar)
  val jars = ListBuffer[AppJar]()
  Option(jarFile.exists()) match {
    case Some(true) =>
      val main = config.remainArgs(0)
      val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" + jarFile.getAbsolutePath)),
        Thread.currentThread().getContextClassLoader())
      val clazz = classLoader.loadClass(main)
      val instance = clazz.newInstance()
      val fis = new FileInputStream(jarFile)
      val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
      val buf = ListBuffer[Byte]()
      var b = fis.read()
      while (b != -1) {
        buf.append(b.byteValue)
        b = fis.read()
      }
      jars += AppJar(jarFile.getName, buf.toArray)
      val start = instance.asInstanceOf[Starter]
      start.main(config.remainArgs.drop(1))
    case Some(false) =>
      LOG.info(s"jar $jar does not exist")
    case None =>
      LOG.info("jar file required")
  }
}
