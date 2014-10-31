package org.apache.gearpump

import java.io.{ByteArrayOutputStream, File, FileInputStream}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

package object streaming {
  private val LOG: Logger = LoggerFactory.getLogger("org.apache.gearpump.streaming")

  type TaskGroup = Int
  type TaskIndex = Int

  implicit def fileToTaskJar(file: File): Option[TaskJar] = {
    if(file.exists) {
      val fis = new FileInputStream(file)
      val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
      val buf = ListBuffer[Byte]()
      var b = fis.read()
      while (b != -1) {
        buf.append(b.byteValue)
        b = fis.read()
      }
      Option(TaskJar(file.getName, buf.toArray))
    } else {
      LOG.error(s"Could not open ${file.getName}")
      None
    }
  }
 }
