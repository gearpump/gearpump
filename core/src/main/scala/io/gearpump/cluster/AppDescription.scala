/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.cluster

import akka.actor.{Actor, ActorRef, ActorSystem}
import com.typesafe.config.{Config, ConfigFactory}
import io.gearpump.cluster.appmaster.WorkerInfo
import io.gearpump.cluster.scheduler.Resource
import io.gearpump.jarstore.FilePath

import scala.reflect.ClassTag

/**
 * This contains all information to run an application
 *
 * @param name: The name of this application
 * @param appMaster: The class name of AppMaster Actor
 * @param userConfig: user configuration.
 * @param clusterConfig: The user provided cluster config, it will override gear.conf when starting
 *  new applications. In most cases, you wouldnot need to change it. If you do need to change it,
 *  use ClusterConfigSource(filePath) to construct the object, while filePath points to the .conf file.
 */

case class AppDescription(name : String, appMaster : String, userConfig: UserConfig, clusterConfig: Config = ConfigFactory.empty())

trait Application {
  def name: String
  def userConfig(implicit system: ActorSystem): UserConfig
  def appMaster: Class[_ <: ApplicationMaster]
}

object Application {
  def apply[T <: ApplicationMaster](name: String, userConfig: UserConfig)(implicit tag: ClassTag[T]): Application = {
    new DefaultApplication(name, userConfig, tag.runtimeClass.asInstanceOf[Class[_ <: ApplicationMaster]])
  }

  class DefaultApplication(override val name: String, inputUserConfig: UserConfig, val appMaster: Class[_ <: ApplicationMaster]) extends Application {
    override def userConfig(implicit system: ActorSystem): UserConfig = inputUserConfig
  }

  def ApplicationToAppDescription(app: Application)(implicit system: ActorSystem): AppDescription = {
    AppDescription(app.name, app.appMaster.getName, app.userConfig, system.settings.config)
  }
}

/**
 * Used for verification. All AppMaster must extend this interface
 */
abstract class ApplicationMaster extends Actor

/**
 * This contains context information when starting an AppMaster
 *
 * @param appId: application instance id assigned, it is unique in the cluster
 * @param username: The username who submitted this application
 * @param resource: Resouce allocated to start this AppMaster daemon. AppMaster are allowed to
 *                request more resource from Master.
 * @param appJar: application Jar. If the jar is already in classpath, then it can be None.
 * @param masterProxy: The proxy to master actor, it will bridge the messages between appmaster and master
 * @param registerData: The AppMaster are required to register this data back to Master by RegisterAppMaster
 *
 */
case class AppMasterContext(
    appId : Int,
    username : String,
    resource : Resource,
    workerInfo: WorkerInfo,
    appJar : Option[AppJar],
    masterProxy : ActorRef,
    registerData : AppMasterRegisterData)

/**
 * Jar file container in the cluster
 *
 * @param name: A meaningful name to represent this jar
 * @param filePath: Where the jar file is stored.
 */
case class AppJar(name: String, filePath: FilePath)


/**
 * TODO: ExecutorContext doesn't belong here.
 * Need to move to other places
 */
case class ExecutorContext(executorId : Int, worker: WorkerInfo, appId : Int, appName: String,
                           appMaster : ActorRef, resource : Resource)


/**
 * TODO: ExecutorJVMConfig doesn't belong here.
 * Need to move to other places
 */
/**
 * @param classPath: When a worker create a executor, the parent worker's classpath will
 * be automatically inherited, the application jar will also be added to runtime
 * classpath automatically. Sometimes, you still want to add some extraclasspath,
 * you can do this by specify classPath option.
 * @param jvmArguments java arguments like -Dxx=yy
 * @param mainClass Executor main class name like io.gearpump.xx.AppMaster
 * @param arguments Executor command line arguments
 * @param jar application jar
 * @param executorAkkaConfig Akka config used to initialize the actor system of this executor. It will
 * use io.gearpump.util.Constants.GEARPUMP_CUSTOM_CONFIG_FILE to pass the config to executor
 * process
 *
 */
case class ExecutorJVMConfig(classPath : Array[String], jvmArguments : Array[String], mainClass : String, arguments : Array[String], jar: Option[AppJar], username : String, executorAkkaConfig: Config = ConfigFactory.empty())