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

package io.gearpump.cluster

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.MasterToClient.{ApplicationFailed, ApplicationResult, ApplicationSucceeded, ApplicationTerminated}
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.jarstore.FilePath
import java.io.Serializable

/**
 * This contains all information to run an application
 *
 * @param name The name of this application
 * @param appMaster The class name of AppMaster Actor
 * @param userConfig user configuration.
 * @param clusterConfig User provided cluster config, it overrides gear.conf when starting
 *                      new applications. In most cases, you should not need to change it. If you do
 *                      really need to change it, please use ClusterConfigSource(filePath) to
 *                      construct the object, while filePath points to the .conf file.
 */
case class AppDescription(name: String, appMaster: String, userConfig: UserConfig,
    clusterConfig: Config = ConfigFactory.empty())

/**
 * Used for verification. All AppMaster must extend this interface
 */
abstract class ApplicationMaster extends Actor

/**
 * This contains context information when starting an AppMaster
 *
 * @param appId application instance id assigned, it is unique in the cluster
 * @param username The username who submitted this application
 * @param resource Resouce allocated to start this AppMaster daemon. AppMaster are allowed to
 *                 request more resource from Master.
 * @param appJar application Jar. If the jar is already in classpath, then it can be None.
 * @param masterProxy The proxy to master actor, it bridges the messages between appmaster
 *                    and master
 */
case class AppMasterContext(
    appId: Int,
    username: String,
    resource: Resource,
    workerInfo: WorkerInfo,
    appJar: Option[AppJar],
    masterProxy: ActorRef)

/**
 * Jar file container in the cluster
 *
 * @param name A meaningful name to represent this jar
 * @param filePath Where the jar file is stored.
 */
case class AppJar(name: String, filePath: FilePath)

/**
 * Serves as the context to start an Executor JVM.
 */
// TODO: ExecutorContext doesn't belong to this package in logic.
case class ExecutorContext(
    executorId: Int, worker: WorkerInfo, appId: Int, appName: String,
    appMaster: ActorRef, resource: Resource)

/**
 * JVM configurations to start an Executor JVM.
 *
 * @param classPath When executor is created by a worker JVM, executor automatically inherits
 *                  parent worker's classpath. Sometimes, you still want to add some extra
 *                  classpath, you can do this by specify classPath option.
 * @param jvmArguments java arguments like -Dxx=yy
 * @param mainClass Executor main class name like io.gearpump.xx.AppMaster
 * @param arguments Executor command line arguments
 * @param jar application jar
 * @param executorAkkaConfig Akka config used to initialize the actor system of this executor.
 *                           It uses io.gearpump.util.Constants.GEARPUMP_CUSTOM_CONFIG_FILE
 *                           to pass the config to executor process
 */
// TODO: ExecutorContext doesn't belong to this package in logic.
case class ExecutorJVMConfig(
    classPath: Array[String], jvmArguments: Array[String], mainClass: String,
    arguments: Array[String], jar: Option[AppJar], username: String,
    executorAkkaConfig: Config = ConfigFactory.empty())

sealed abstract class ApplicationStatus(val status: String)
  extends Serializable{
  override def toString: String = status

  def canTransitTo(newStatus: ApplicationStatus): Boolean

}

sealed abstract class ApplicationTerminalStatus(override val status: String)
  extends ApplicationStatus(status) {

  override def canTransitTo(newStatus: ApplicationStatus): Boolean = false
}

object ApplicationStatus {
  case object PENDING extends ApplicationStatus("pending") {
    override def canTransitTo(newStatus: ApplicationStatus): Boolean = {
      !newStatus.equals(NONEXIST)
    }
  }

  case object ACTIVE extends ApplicationStatus("active") {
    override def canTransitTo(newStatus: ApplicationStatus): Boolean = {
      !newStatus.equals(NONEXIST) && !newStatus.equals(ACTIVE)
    }
  }

  case object SUCCEEDED extends ApplicationTerminalStatus("succeeded")

  case class FAILED(ex: Throwable) extends ApplicationTerminalStatus("failed")

  case object TERMINATED extends ApplicationTerminalStatus("terminated")

  case object NONEXIST extends ApplicationStatus("nonexist") {
    override def canTransitTo(newStatus: ApplicationStatus): Boolean = false
  }

  def toResult(status: ApplicationTerminalStatus, appId: Int): ApplicationResult = {
    status match {
      case SUCCEEDED =>
        ApplicationSucceeded(appId)
      case FAILED(ex) =>
        ApplicationFailed(appId, ex)
      case TERMINATED =>
        ApplicationTerminated(appId)
    }
  }
}
