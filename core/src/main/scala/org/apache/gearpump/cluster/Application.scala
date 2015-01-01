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

case class AppJar(name: String, container: JarFileContainer)

/**
 * Subtype this for user defined application
 */
trait Application {
  val name : String
  val appMaster : String
  val conf: UserConfig
}

/**
 * Used for verification
 */
abstract class ApplicationMaster extends Actor

/**
 * Used for verification
 */
abstract class ApplicationExecutor extends Actor


/**
 * This interface should keep stable, so that we can remain
 * compatible with old versions
 */
trait ExecutorContextInterface {
  val executorId : Int
  val workerId : Int
  val appId : Int
  val appMaster : ActorRef
  val startClock : TimeStamp
  val resource : Resource
}

case class ExecutorContext(executorId : Int, workerId: Int, appId : Int,
                           appMaster : ActorRef, startClock : TimeStamp, resource : Resource) extends ExecutorContextInterface

/**
 * This belongs to the app submission interface, no method should
 * be removed to remain compatible with old version.
 */
trait AppMasterContextInterface {
  val appId : Int
  val username : String
  val masterExecutorId : Int
  val resource : Resource
  val appJar : Option[AppJar]
  val masterProxy : ActorRef
  val registerData : AppMasterRegisterData
}

case class AppMasterContext(appId : Int, username : String, masterExecutorId : Int,
                            resource : Resource,  appJar : Option[AppJar],
                            masterProxy : ActorRef,  registerData : AppMasterRegisterData)
  extends AppMasterContextInterface