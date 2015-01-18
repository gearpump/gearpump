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

package org.apache.gearpump.cluster

import akka.actor.{Actor, ActorRef}
import org.apache.gearpump._
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.jarstore.JarFileContainer
import com.typesafe.config.Config

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

final class Application(val name : String, val appMaster : String, val userConfig: UserConfig, val clusterConfig: ClusterConfigSource = null) extends Serializable

object Application {
  def apply(name : String, appMaster : String, conf: UserConfig, appMasterClusterConfig: ClusterConfigSource = null) : Application = new Application(name, appMaster, conf, appMasterClusterConfig)
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
 * @param registerData: The AppMaster are required to register this data back to Master by [[org.apache.gearpump.cluster.AppMasterToMaster.RegisterAppMaster]]
 *
 */
case class AppMasterContext(appId : Int, username : String,
                                  resource : Resource,  appJar : Option[AppJar],
                                  masterProxy : ActorRef,  registerData : AppMasterRegisterData)

/**
 * Jar file container in the cluster
 *
 * @param name: A meaningful name to represent this jar
 * @param container: Where the jar file is stored.
 */
case class AppJar(name: String, container: JarFileContainer)


/**
 * TODO: ExecutorContext doesn't belong here.
 * Need to move to other places
 */
case class ExecutorContext(executorId : Int, workerId: Int, appId : Int,
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
 * @param mainClass Executor main class name like org.apache.gearpump.xx.AppMaster
 * @param arguments Executor command line arguments
 * @param jar application jar
 * @param executorAkkaConfig Akka config used to initialize the actor system of this executor. It will
 * use [[org.apache.gearpump.util.Constants.GEARPUMP_CUSTOM_CONFIG_FILE]] to pass the config to executor
 * process
 *
 */
case class ExecutorJVMConfig(classPath : Array[String], jvmArguments : Array[String], mainClass : String, arguments : Array[String], jar: Option[AppJar], username : String, executorAkkaConfig: Config = null)