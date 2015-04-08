package org.apache.gearpump.util

import java.io.File
import java.net.InetAddress
import java.util.Properties

import com.typesafe.config.Config
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object LogUtil {
  object ProcessType extends Enumeration {
    type ProcessType = Value
    val MASTER, WORKER, LOCAL, APPLICATION, UI = Value
  }

  def getLogger[T](clazz : Class[T], context : String = null, master : Any = null, worker : Any = null, executor : Any = null, task : Any = null, app : Any = null) : Logger = {

    var env = ""

    if (null != context) {
      env += context
    }
    if (null != master) {
      env += "master" + master
    }
    if (null != worker) {
      env += "worker" + worker
    }

    if (null != app) {
      env += "app" + app
    }

    if (null != executor) {
      env += "exec" + executor
    }
    if (null != task) {
      env += task
    }

    if (!env.isEmpty) {
      LoggerFactory.getLogger(clazz.getSimpleName + "@" + env)
    } else {
      LoggerFactory.getLogger(clazz.getSimpleName)
    }
  }

  def loadConfiguration(config : Config, processType : ProcessType.ProcessType) : Unit = {
    setHostnameSystemProperty

    //set log file name
    val propName = s"gearpump.${processType.toString.toLowerCase}.log.file"
    System.setProperty("gearpump.log.file", "${" + propName + "}")

    val props = new Properties()
    val log4jConfStream = getClass().getClassLoader.getResourceAsStream("log4j.properties")
    if(log4jConfStream!=null) {
      props.load(log4jConfStream)
    }

    processType match {
      case ProcessType.APPLICATION =>
        props.setProperty("log4j.rootLogger", "${gearpump.application.logger}")
        props.setProperty("gearpump.application.log.rootdir", applicationLogDir(config).getAbsolutePath)
      case _ =>
        props.setProperty("log4j.rootLogger", "${gearpump.root.logger}")
        props.setProperty("gearpump.log.dir", daemonLogDir(config).getAbsolutePath)
    }

    PropertyConfigurator.configure(props)
  }

  def daemonLogDir(config: Config): File = {
    val dir = config.getString(Constants.GEARPUMP_LOG_DAEMON_DIR)
    new File(dir)
  }

  private def setHostnameSystemProperty : Unit = {
    val hostname = Try(InetAddress.getLocalHost.getHostName).getOrElse("local")
    val jvmIdentity = java.lang.management.ManagementFactory.getRuntimeMXBean().getName()
    //as log4j missing the HOSTNAME system property, add it to system property, just like logback does
    System.setProperty("JVM_NAME", jvmIdentity)
  }

  def applicationLogDir(config: Config): File = {
    val appLogDir = config.getString(Constants.GEARPUMP_LOG_APPLICATION_DIR)
    new File(appLogDir)
  }
}
