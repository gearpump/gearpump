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
package org.apache.gearpump.experiments.yarn.client

import org.apache.gearpump.util.LogUtil
import org.slf4j.Logger

/** Command line tool to launch a Gearpump cluster on YARN, and also to manage Gearpump cluster */
object Client {

  private val LOG: Logger = LogUtil.getLogger(getClass)
  val LAUNCH = "launch"

  val commands = Map(LAUNCH -> LaunchCluster) ++
    ManageCluster.commands.map(key => (key, ManageCluster)).toMap

  def usage(): Unit = {
    val keys = commands.keys.toList.sorted
    // scalastyle:off println
    Console.err.println("Usage: " + "<" + keys.mkString("|") + ">")
    // scalastyle:on println
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      usage()
    } else {
      val key = args(0)
      val command = commands.get(key)
      command match {
        case Some(command) =>
          if (key == LAUNCH) {
            val remainArgs = args.drop(1)
            command.main(remainArgs)
          } else {
            val commandArg = Array("-" + ManageCluster.COMMAND, key)
            val remainArgs = args.drop(1)
            val updatedArgs = commandArg ++ args.drop(1)
            command.main(updatedArgs)
          }
        case None =>
          usage
      }
    }
  }
}