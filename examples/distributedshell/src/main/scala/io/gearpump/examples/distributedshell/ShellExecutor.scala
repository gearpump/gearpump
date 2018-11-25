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

package io.gearpump.examples.distributedshell

import scala.sys.process._
import scala.util.{Failure, Success, Try}
import akka.actor.Actor
import org.slf4j.Logger
import io.gearpump.cluster.ExecutorContext
import DistShellAppMaster.{ShellCommand, ShellCommandResult}
import io.gearpump.cluster.UserConfig
import io.gearpump.util.LogUtil

/** Executor actor on remote machine */
class ShellExecutor(executorContext: ExecutorContext, userConf: UserConfig) extends Actor {
  import executorContext._
  private val LOG: Logger = LogUtil.getLogger(getClass, executor = executorId, app = appId)

  LOG.info(s"ShellExecutor started!")

  override def receive: Receive = {
    case ShellCommand(command) =>
      val process = Try(s"$command".!!)
      val result = process match {
        case Success(msg) => msg
        case Failure(ex) => ex.getMessage
      }
      sender ! ShellCommandResult(executorId, result)
  }
}
