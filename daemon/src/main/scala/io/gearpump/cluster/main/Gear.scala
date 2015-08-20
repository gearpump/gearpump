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
package io.gearpump.cluster.main

import io.gearpump.util.LogUtil
import org.slf4j.Logger

object Gear  {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  val commands = Map("app" -> AppSubmitter, "kill" -> Kill,
    "info" -> Info, "replay" -> Replay, "main" -> MainRunner)

  def usage: Unit = {
    val keys = commands.keys.toList.sorted
    Console.println("Usage: " + "<" + keys.mkString("|") + ">")
  }

  def executeCommand(command : String, commandArgs : Array[String]) = {
    commands.get(command).map(_.main(commandArgs))
    if (!commands.contains(command)) {
      val allArgs = (command +: commandArgs.toList).toArray
      MainRunner.main(allArgs)
    }
  }

  def main(args: Array[String]) = {
    args.length match {
      case 0 =>
        usage
      case a if(a >= 1) =>
        val command = args(0)
        val commandArgs = args.drop(1)
        executeCommand(command, commandArgs)
    }
  }
}