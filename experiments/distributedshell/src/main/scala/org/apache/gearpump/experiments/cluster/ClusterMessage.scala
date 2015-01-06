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
package org.apache.gearpump.experiments.cluster

import akka.actor.{ActorRef, Actor}
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.scheduler.Resource
import org.apache.gearpump.experiments.cluster.task.TaskContextInterface

object AppMasterToExecutor {
  case class LaunchTask(taskContext: TaskContextInterface, taskClass: Class[_ <: Actor], userConfig: UserConfig)
  case class MsgToTask(msg: Any)
}

object ExecutorToAppMaster {
  case class RegisterExecutor(executor: ActorRef, executorId: Int, resource: Resource, workerId : Int)
  case class ResponsesFromTasks(executorId: Int, msgs: List[Any])
}
