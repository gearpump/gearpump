package org.apache.gearpump

import java.io.{ByteArrayOutputStream, FileInputStream}
import java.net.URLClassLoader

import org.apache.gearpump.streaming.task.TaskActor
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable

package object streaming {
  private val LOG: Logger = LoggerFactory.getLogger("org.apache.gearpump.streaming")

  type TaskGroup = Int
  type TaskIndex = Int

  implicit def classToTaskJar(clazz: Class[_<:TaskActor]): String = {
    LOG.info("in classToTaskJar")
    val classLoader = clazz.getClassLoader;
    val urlClassLoader = classLoader.asInstanceOf[URLClassLoader]
    val file = urlClassLoader.getURLs().map(url => {
      LOG.info(s"url.getFile ${url.getFile}")
      val file = new java.io.File(url.getFile)
      if (file.exists) {
        val fis = new FileInputStream(file)
        val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
        val buf = ListBuffer[Byte]()
        var b = fis.read()
        while (b != -1) {
          buf.append(b.byteValue)
          b = fis.read()
        }
        if (!JarsForTasks.jars.contains(clazz.getCanonicalName)) {
          LOG.info(s"JarsForTasks setting ${clazz.getCanonicalName}")
          JarsForTasks.jars += (clazz.getCanonicalName -> TaskJar(file.getName, buf.toArray))
        } else {
          LOG.error(s"Could not open ${file.getName}")
          None
        }
      }
    })
    clazz.getCanonicalName
  }

  object JarsForTasks {
    val jars = scala.collection.mutable.Map[String, TaskJar]()
  }
}
