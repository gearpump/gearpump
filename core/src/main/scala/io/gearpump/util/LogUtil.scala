/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.util

import com.typesafe.config.Config
import java.io.File
import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import org.slf4j.{Logger, LoggerFactory}

object LogUtil {
  object ProcessType extends Enumeration {
    type ProcessType = Value
    val MASTER, WORKER, LOCAL, APPLICATION, UI, CLIENT = Value
  }

  def getLogger[T](
      clazz: Class[T], context: String = null, master: Any = null, worker: Any = null,
      executor: Any = null, task: Any = null, app: Any = null, name: String = null): Logger = {
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
    if (null != name) {
      env += name
    }

    if (!env.isEmpty) {
      LoggerFactory.getLogger(clazz.getSimpleName + "@" + env)
    } else {
      LoggerFactory.getLogger(clazz.getSimpleName)
    }
  }

  /** getLogger(Class) implementation for Java */
  def getLogger[T](clazz: Class[T]): Logger = {
    LoggerFactory.getLogger(clazz.getSimpleName)
  }

  /** Custom the log file locations by reading config from system properties */
  def loadConfiguration(config: Config, processType: ProcessType.ProcessType): Unit = {
    // Set log file name
    val propName = s"gearpump.${processType.toString.toLowerCase}.log.file"
    val props = loadConfiguration

    props.setProperty("gearpump.log.file", "${" + propName + "}")

    props.setProperty("JVM_NAME", jvmName)

    processType match {
      case ProcessType.APPLICATION =>
        props.setProperty("log4j.rootAppender", f"$${gearpump.application.logger}")
        props.setProperty("gearpump.application.log.rootdir",
          applicationLogDir(config).getAbsolutePath)
      case _ =>
        props.setProperty("log4j.rootAppender", f"$${gearpump.root.logger}")
        props.setProperty("gearpump.log.dir", daemonLogDir(config).getAbsolutePath)
    }

    PropertyConfigurator.configure(props)
  }

  def daemonLogDir(config: Config): File = {
    Util.asSubDirOfGearpumpHome(config.getString(Constants.GEARPUMP_LOG_DAEMON_DIR))
  }

  def verboseLogToConsole(): Unit = {
    val props = loadConfiguration
    props.setProperty("log4j.rootLogger", "DEBUG,console")
    PropertyConfigurator.configure(props)
  }

  def loadConfiguration: Properties = {
    val props = new Properties()
    val log4jConfStream = getClass().getClassLoader.getResourceAsStream("log4j.properties")
    if (log4jConfStream != null) {
      props.load(log4jConfStream)
    }
    log4jConfStream.close()
    props
  }

  private def jvmName: String = {
    java.lang.management.ManagementFactory.getRuntimeMXBean().getName()
  }

  def applicationLogDir(config: Config): File = {
    Util.asSubDirOfGearpumpHome(config.getString(Constants.GEARPUMP_LOG_APPLICATION_DIR))
  }
}