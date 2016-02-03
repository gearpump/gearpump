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

package io.gearpump.experiments.storm

import io.gearpump.experiments.storm.main.{GearpumpNimbus, GearpumpStormClient}
import io.gearpump.util.LogUtil
import org.slf4j.Logger

object StormRunner {
  private val LOG: Logger = LogUtil.getLogger(getClass)

  private val commands = Map("nimbus" -> GearpumpNimbus, "app" -> GearpumpStormClient)

  private def usage: Unit = {
    val keys = commands.keys.toList.sorted
    Console.err.println("Usage: " + "<" + keys.mkString("|") + ">")
  }

  private def executeCommand(command : String, commandArgs : Array[String]): Unit = {
    if (!commands.contains(command)) {
      usage
    } else {
      commands(command).main(commandArgs)
    }
  }

  def main(args: Array[String]) = {
    if (args.length == 0) {
      usage
    } else {
      val command = args(0)
      val commandArgs = args.drop(1)
      executeCommand(command, commandArgs)
    }
  }
}
