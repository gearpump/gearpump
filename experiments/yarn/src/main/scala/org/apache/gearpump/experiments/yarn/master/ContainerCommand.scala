package org.apache.gearpump.experiments.yarn.master

import java.io.File

import org.apache.gearpump.experiments.yarn.AppConfig
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants
import org.apache.hadoop.yarn.api.ApplicationConstants


trait ContainerCommand {
  protected val appConfig: AppConfig
  private val version = appConfig.getEnv("version")
  private val classPath = Array(
    s"pack/$version/conf",
    s"pack/$version/dashboard",
    s"pack/$version/lib/*",
    s"pack/$version/daemon/*"
  )

  def getCommand:String

  protected def buildCommand(java: String, properties: Array[String], mainProp: String, cliOpts: String, lognameProp: String):String = {
    val exe = appConfig.getEnv(java)
    val main = appConfig.getEnv(mainProp)
    val logname = appConfig.getEnv(lognameProp)
    s"$exe -cp ${classPath.mkString(File.pathSeparator)}${File.pathSeparator}" +
      "$CLASSPATH " + properties.mkString(" ") +
      s"  $main $cliOpts 2>&1 | /usr/bin/tee -a ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/$logname"
  }
}


case class MasterContainerCommand(appConfig: AppConfig, masterAddr: HostPort) extends ContainerCommand {

  def getCommand: String = {
    val masterArguments = s"-ip ${masterAddr.host} -port ${masterAddr.port}"

    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=${masterAddr.host}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}")

    buildCommand(GEARPUMPMASTER_COMMAND, properties, GEARPUMPMASTER_MAIN,
      masterArguments, GEARPUMPMASTER_LOG)
  }
}

case class WorkerContainerCommand(appConfig: AppConfig, masterAddr: HostPort, workerHost: String) extends ContainerCommand {

  def getCommand: String = {
    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${ApplicationConstants.LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$workerHost")

    buildCommand(WORKER_COMMAND, properties,  WORKER_MAIN, "", WORKER_LOG)
  }


}
