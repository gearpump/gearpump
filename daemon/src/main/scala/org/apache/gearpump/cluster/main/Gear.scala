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
package org.apache.gearpump.cluster.main

import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.util.Try

object Gear extends App {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  def usage(commandOption: Option[String]) = {
    commandOption match {
      case Some(command) =>
        command match {
          case "kill" =>
            Kill.main(Array.empty[String])
          case "shell" =>
            Shell.main(Array.empty[String])
          case "info" =>
            Info.main(Array.empty[String])
          case "replay" =>
            Replay.main(Array.empty[String])
          case "app" =>
            AppSubmitter.main(Array.empty[String])
          case x =>
            throw new Exception("Unknown command " + x)
        }
      case None =>
        Console.println("Usage: app|info|kill|shell|replay|mainClass ...")
    }
  }

  def executeCommand(command : String, commandArgs : Array[String]) = {

    command match {
      case "kill" =>
        Kill.main(commandArgs)
      case "shell" =>
        Shell.main(commandArgs)
      case "info" =>
        Info.main(commandArgs)
      case "replay" =>
        Replay.main(commandArgs)
      case "app" =>
        AppSubmitter.main(commandArgs)
      case main =>
        val customCommand = Try {
          val clazz = Thread.currentThread().getContextClassLoader().loadClass(main)
          val mainMethod = clazz.getMethod("main", classOf[Array[String]])
          mainMethod.invoke(null, commandArgs)
        }
        if (customCommand.isFailure) {
          val ex = customCommand.failed.get
          LOG.error(s"failed to execute command $main", ex)
          throw ex
        }
    }
  }

  def start = {
    args.length match {
      case 0 =>
        usage(None)
      case a if(a >= 1) =>
        val command = args(0)
        val commandArgs = args.drop(1)
        executeCommand(command, commandArgs)
    }
  }

  start
}
