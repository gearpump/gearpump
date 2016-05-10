/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

package org.apache.gearpump.experiments.yarn.appmaster

import com.typesafe.config.Config

import org.apache.gearpump.cluster.main.{Master, Worker}
import org.apache.gearpump.experiments.yarn.Constants._
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.Constants

/** Command to start a YARN container */
trait Command {
  def get: String
  override def toString: String = get
}

abstract class AbstractCommand extends Command {
  protected def config: Config
  def version: String
  def classPath: Array[String] = {
    Array(
      s"conf",
      s"pack/$version/conf",
      s"pack/$version/lib/daemon/*",
      s"pack/$version/lib/*"
    )
  }

  protected def buildCommand(
      java: String, properties: Array[String], mainClazz: String, cliOpts: Array[String])
    : String = {
    val exe = config.getString(java)

    s"$exe -cp ${classPath.mkString(":")}:" +
      "$CLASSPATH " + properties.mkString(" ") +
      s" $mainClazz ${cliOpts.mkString(" ")} 2>&1 | /usr/bin/tee -a ${LOG_DIR_EXPANSION_VAR}/stderr"
  }

  protected def clazz(any: AnyRef): String = {
    val name = any.getClass.getName
    if (name.endsWith("$")) {
      name.dropRight(1)
    } else {
      name
    }
  }
}

case class MasterCommand(config: Config, version: String, masterAddr: HostPort)
  extends AbstractCommand {

  def get: String = {
    val masterArguments = Array(s"-ip ${masterAddr.host}", s"-port ${masterAddr.port}")

    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=${masterAddr.host}",
      s"-D${Constants.GEARPUMP_MASTER_RESOURCE_MANAGER_CONTAINER_ID}=${CONTAINER_ID}",
      s"-D${Constants.GEARPUMP_HOME}=${LOCAL_DIRS}/${CONTAINER_ID}/pack/$version",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${LOG_DIR_EXPANSION_VAR}")

    buildCommand(MASTER_COMMAND, properties, clazz(Master), masterArguments)
  }
}

case class WorkerCommand(config: Config, version: String, masterAddr: HostPort, workerHost: String)
  extends AbstractCommand {

  def get: String = {
    val properties = Array(
      s"-D${Constants.GEARPUMP_CLUSTER_MASTERS}.0=${masterAddr.host}:${masterAddr.port}",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_WORKER_RESOURCE_MANAGER_CONTAINER_ID}=${CONTAINER_ID}",
      s"-D${Constants.GEARPUMP_HOME}=${LOCAL_DIRS}/${CONTAINER_ID}/pack/$version",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=$workerHost")

    buildCommand(WORKER_COMMAND, properties, clazz(Worker), Array.empty[String])
  }
}

case class AppMasterCommand(config: Config, version: String, args: Array[String])
  extends AbstractCommand {

  override val classPath = Array(
    "conf",
    s"pack/$version/conf",
    s"pack/$version/dashboard",
    s"pack/$version/lib/*",
    s"pack/$version/lib/daemon/*",
    s"pack/$version/lib/services/*",
    s"pack/$version/lib/yarn/*"
  )

  def get: String = {
    val properties = Array(
      s"-D${Constants.GEARPUMP_HOME}=${LOCAL_DIRS}/${CONTAINER_ID}/pack/$version",
      s"-D${Constants.GEARPUMP_FULL_SCALA_VERSION}=$version",
      s"-D${Constants.GEARPUMP_LOG_DAEMON_DIR}=${LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_LOG_APPLICATION_DIR}=${LOG_DIR_EXPANSION_VAR}",
      s"-D${Constants.GEARPUMP_HOSTNAME}=${NODEMANAGER_HOST}")

    val arguments = Array(s"") ++ args

    buildCommand(APPMASTER_COMMAND, properties, clazz(YarnAppMaster),
      arguments)
  }
}