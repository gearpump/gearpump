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
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.cluster.task.TaskContextInterface
import org.slf4j.{LoggerFactory, Logger}
import sys.process._

class ShellTask(taskContext : TaskContextInterface, userConf : UserConfig) extends Actor {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)

  LOG.info(s"ShellTask started!")

  override def receive: Receive = {
    case ShellCommand(command, args) =>
      val result = s"$command $args" !!

      LOG.info(s"Task execute shell command '$command $args', result is $result")
      sender ! result
  }
}
