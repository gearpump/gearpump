package org.apache.gearpump.util

import java.io.File

object Util {
  def getCurrentClassPath : Array[String] = {
    val classpath = System.getProperty("java.class.path");
    val classpathList = classpath.split(File.pathSeparator);
    classpathList
  }
}
