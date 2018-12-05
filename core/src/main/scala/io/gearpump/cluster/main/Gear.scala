/*
 * Licensed under the Apache License, Version 2.0 (the
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
package io.gearpump.cluster.main

import io.gearpump.util.Constants

object Gear {

  val OPTION_CONFIG = "conf"

  val commands = Map("app" -> AppSubmitter, "kill" -> Kill,
    "info" -> Info, "replay" -> Replay, "main" -> MainRunner)

  def printUsage(): Unit = {
    val keys = commands.keys.toList.sorted
    // scalastyle:off println
    Console.err.println("Usage: " + "<" + keys.mkString("|") + ">")
    // scalastyle:on println
  }

  private def executeCommand(command: String, commandArgs: Array[String]) = {
    commands.get(command) match {
      case Some(runner) =>
        runner.main(commandArgs)
      case None =>
        printUsage()
    }
  }

  def main(inputArgs: Array[String]): Unit = {
    val (configFile, args) = extractConfig(inputArgs)
    if (configFile != null) {
      // Sets custom config file...
      System.setProperty(Constants.GEARPUMP_CUSTOM_CONFIG_FILE, configFile)
    }

    if (args.length == 0) {
      printUsage()
    } else {
      val command = args(0)
      val commandArgs = args.drop(1)
      executeCommand(command, commandArgs)
    }
  }

  private def extractConfig(inputArgs: Array[String]): (String, Array[String]) = {
    var index = 0

    var result = List.empty[String]
    var configFile: String = null
    while (index < inputArgs.length) {
      val item = inputArgs(index)
      if (item == s"-$OPTION_CONFIG") {
        index += 1
        configFile = inputArgs(index)
      } else {
        result = result :+ item
      }
      index += 1
    }
    (configFile, result.toArray)
  }
}