package org.apache.gearpump

import java.io.File

/**
 * Created by xzhong10 on 2014/7/22.
 */
trait ExecutorContext extends Serializable {
  def getClassPath() : Array[String]
}

class DefaultExecutorContext extends ExecutorContext {
  def getClassPath() : Array[String] = {
    val classpath = System.getProperty("java.class.path");
    val classpathList = classpath.split(File.pathSeparator);
    classpathList
  }
}