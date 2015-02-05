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

package org.apache.gearpump.distributedshell

import akka.actor.Actor
import org.apache.gearpump.cluster.{UserConfig, ExecutorContext}
import org.apache.gearpump.distributedshell.ShellExecutor.ShellCommand
import org.apache.gearpump.experiments.cluster.ExecutorToAppMaster.{ResponsesFromExecutor, RegisterExecutor}
import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.util.{Failure, Success, Try}
import sys.process._

class ShellExecutor(executorContext: ExecutorContext, userConf : UserConfig) extends Actor{
  import executorContext._
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  appMaster ! RegisterExecutor(self, executorId, resource, workerId)
  context.watch(appMaster)
  LOG.info(s"ShellExecutor started!")

  override def receive: Receive = {
    case ShellCommand(command, args) =>
      val process = Try(s"$command $args" !!)
      val result = process match {
        case Success(msg) => msg
        case Failure(ex) => ex.getMessage
      }
      sender ! ResponsesFromExecutor(executorId, List(result))
  }
}

object ShellExecutor{
  case class ShellCommand(command: String, args: String)
}
